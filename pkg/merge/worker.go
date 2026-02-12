// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package merge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/storage"
)

// MergeWorkerConfig contains configuration for the merge worker
type MergeWorkerConfig struct {
	WorkerID      string
	TmpDir        string
	PollInterval  time.Duration
	StorageConfig *config.StorageConfig
}

// MergeWorker processes merge jobs from the queue
type MergeWorker struct {
	config   *MergeWorkerConfig
	queue    *MergeQueue
	storage  storage.Storage
	shutdown chan struct{}
}

// NewMergeWorker creates a new merge worker
func NewMergeWorker(redisClient redis.UniversalClient, cfg *MergeWorkerConfig) (*MergeWorker, error) {
	if cfg.WorkerID == "" {
		cfg.WorkerID = fmt.Sprintf("merge-worker-%d", time.Now().UnixNano())
	}
	if cfg.TmpDir == "" {
		cfg.TmpDir = os.TempDir()
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}

	var store storage.Storage
	var err error
	if cfg.StorageConfig != nil {
		store, err = getStorage(cfg.StorageConfig)
		if err != nil {
			return nil, err
		}
	}

	return &MergeWorker{
		config:   cfg,
		queue:    NewMergeQueue(redisClient),
		storage:  store,
		shutdown: make(chan struct{}),
	}, nil
}

// getStorage creates a storage instance from config
func getStorage(conf *config.StorageConfig) (storage.Storage, error) {
	switch {
	case conf.S3 != nil:
		return storage.NewS3(conf.S3)
	case conf.GCP != nil:
		return storage.NewGCP(conf.GCP)
	case conf.Azure != nil:
		return storage.NewAzure(conf.Azure)
	case conf.AliOSS != nil:
		return storage.NewAliOSS(conf.AliOSS)
	default:
		return storage.NewLocal(&storage.LocalConfig{})
	}
}

// Run starts the merge worker processing loop
func (w *MergeWorker) Run(ctx context.Context) error {
	logger.Infow("merge worker started", "workerID", w.config.WorkerID)

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Infow("merge worker shutting down", "workerID", w.config.WorkerID)
			return ctx.Err()

		case <-w.shutdown:
			logger.Infow("merge worker stopped", "workerID", w.config.WorkerID)
			return nil

		case <-ticker.C:
			if err := w.processNextJob(ctx); err != nil {
				logger.Errorw("error processing job", err, "workerID", w.config.WorkerID)
			}
		}
	}
}

// Stop signals the worker to stop
func (w *MergeWorker) Stop() {
	close(w.shutdown)
}

// processNextJob processes the next job from the queue
func (w *MergeWorker) processNextJob(ctx context.Context) error {
	job, err := w.queue.Dequeue(ctx, w.config.WorkerID)
	if err != nil {
		return err
	}
	if job == nil {
		return nil // No jobs available
	}

	logger.Infow("processing merge job",
		"jobID", job.ID,
		"sessionID", job.SessionID,
	)

	if err := w.processJob(ctx, job); err != nil {
		return w.queue.Fail(ctx, job, err)
	}

	return w.queue.Complete(ctx, job)
}

// ProcessMergeJob processes a single merge job using the provided config and storage.
// This can be called by both the queue-based MergeWorker and the InProcessMergeEnqueuer.
func ProcessMergeJob(ctx context.Context, cfg *MergeWorkerConfig, store storage.Storage, job *MergeJob) error {
	w := &MergeWorker{config: cfg, storage: store}
	return w.processJob(ctx, job)
}

// processJob processes a single merge job
func (w *MergeWorker) processJob(ctx context.Context, job *MergeJob) error {
	// Create temp directory for this job
	jobTmpDir := path.Join(w.config.TmpDir, job.ID)
	if err := os.MkdirAll(jobTmpDir, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(jobTmpDir)

	// 1. Download manifest
	manifest, err := w.downloadManifest(ctx, job.ManifestPath, jobTmpDir)
	if err != nil {
		return fmt.Errorf("failed to download manifest: %w", err)
	}

	// 2. Download all participant files
	participantFiles, err := w.downloadParticipantFiles(ctx, manifest, jobTmpDir)
	if err != nil {
		return fmt.Errorf("failed to download participant files: %w", err)
	}

	if len(participantFiles) == 0 {
		return errors.New("no participant files to merge")
	}

	// 3. Compute alignment
	alignment := ComputeAlignment(manifest)
	if err := ValidateAlignment(alignment); err != nil {
		logger.Warnw("alignment validation warning", err)
	}

	// 4. Build and run merge pipeline
	mergedFiles, err := w.mergeTracks(ctx, manifest, participantFiles, alignment, jobTmpDir)
	if err != nil {
		return fmt.Errorf("failed to merge tracks: %w", err)
	}

	// 5. Upload merged files
	if err := w.uploadMergedFiles(ctx, manifest, mergedFiles, job.ManifestPath); err != nil {
		return fmt.Errorf("failed to upload merged files: %w", err)
	}

	// 6. Update manifest with merge results
	if err := w.updateManifestWithMergeResults(ctx, job.ManifestPath, manifest); err != nil {
		return fmt.Errorf("failed to update manifest: %w", err)
	}

	logger.Infow("merge job completed successfully",
		"jobID", job.ID,
		"mergedFiles", len(mergedFiles),
		"manifestPath", job.ManifestPath,
	)

	return nil
}

// downloadManifest downloads and parses the manifest file
func (w *MergeWorker) downloadManifest(ctx context.Context, manifestPath string, tmpDir string) (*config.AudioRecordingManifest, error) {
	readPath := manifestPath
	if w.hasRemoteStorage() {
		localPath := path.Join(tmpDir, "manifest.json")
		if err := w.downloadFile(ctx, manifestPath, localPath); err != nil {
			return nil, err
		}
		readPath = localPath
	}

	data, err := os.ReadFile(readPath)
	if err != nil {
		return nil, err
	}

	var manifest config.AudioRecordingManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// downloadParticipantFiles downloads all participant audio files
func (w *MergeWorker) downloadParticipantFiles(ctx context.Context, manifest *config.AudioRecordingManifest, tmpDir string) (map[string]string, error) {
	files := make(map[string]string) // participantID -> local file path

	for _, p := range manifest.Participants {
		if len(p.Artifacts) == 0 {
			continue
		}

		// Download the first artifact (prefer OGG if available)
		var artifact *config.AudioArtifact
		for _, a := range p.Artifacts {
			if a.Format == types.AudioRecordingFormatOGGOpus {
				artifact = a
				break
			}
		}
		if artifact == nil {
			artifact = p.Artifacts[0]
		}

		if w.hasRemoteStorage() {
			localPath := path.Join(tmpDir, fmt.Sprintf("%s_%s", p.ParticipantID, artifact.Filename))
			if err := w.downloadFile(ctx, artifact.StorageURI, localPath); err != nil {
				logger.Warnw("failed to download participant file", err, "participantID", p.ParticipantID)
				continue
			}
			files[p.ParticipantID] = localPath
		} else {
			// Local storage: use the storage URI directly as it's a local path
			files[p.ParticipantID] = artifact.StorageURI
		}
	}

	return files, nil
}

// downloadFile downloads a file from storage
func (w *MergeWorker) downloadFile(_ context.Context, remotePath, localPath string) error {
	if err := w.ensureStorage(); err != nil {
		return err
	}

	if w.storage == nil {
		return errors.New("remote storage is not configured")
	}

	_, err := w.storage.DownloadFile(localPath, w.normalizeRemotePath(remotePath))
	return err
}

// mergeTracks merges participant tracks using GStreamer
func (w *MergeWorker) mergeTracks(ctx context.Context, manifest *config.AudioRecordingManifest, participantFiles map[string]string, alignment *AlignmentResult, tmpDir string) (map[types.AudioRecordingFormat]string, error) {
	mergedFiles := make(map[types.AudioRecordingFormat]string)

	// Build GStreamer pipeline for merging
	// Pipeline: filesrc -> decoder -> identity(ts-offset) -> interleave -> encoder -> filesink
	for _, format := range manifest.Formats {
		outputPath := path.Join(tmpDir, fmt.Sprintf("room_mix%s", config.GetFileExtensionForFormat(format)))

		if err := w.runMergePipeline(ctx, participantFiles, alignment, format, outputPath, manifest.SampleRate); err != nil {
			return nil, err
		}

		mergedFiles[format] = outputPath
	}

	return mergedFiles, nil
}

// runMergePipeline runs the GStreamer merge pipeline
func (w *MergeWorker) runMergePipeline(ctx context.Context, participantFiles map[string]string, alignment *AlignmentResult, format types.AudioRecordingFormat, outputPath string, sampleRate int32) error {
	// Build gst-launch command for merging
	// For each participant: filesrc ! decodebin ! audioconvert ! audioresample ! identity ts-offset=X ! interleave.
	// interleave ! audioconvert ! audioresample ! encoder ! filesink

	args := []string{"-e"} // Send EOS on interrupt
	encodeSampleRate := sampleRate
	if format == types.AudioRecordingFormatOGGOpus {
		encodeSampleRate = nearestOpusRate(sampleRate)
	}

	participantOrder := orderedParticipantIDs(participantFiles, alignment)
	if len(participantOrder) == 0 {
		return errors.New("no participant files to merge")
	}

	offsetByParticipant := make(map[string]int64, len(participantOrder))
	if alignment != nil {
		for _, a := range alignment.Alignments {
			offsetByParticipant[a.ParticipantID] = a.GetIdentityTsOffset()
		}
	}

	// Add one mono source chain for each participant. Interleave maps each input
	// to its own output channel in the order we connect the pads.
	for _, participantID := range participantOrder {
		filePath := participantFiles[participantID]
		offset := offsetByParticipant[participantID]

		// Add source chain for this participant
		args = append(args,
			"filesrc", fmt.Sprintf("location=%s", filePath), "!",
			"decodebin", "!",
			"audioconvert", "!",
			"audioresample", "!",
			fmt.Sprintf("audio/x-raw,rate=%d,channels=1,format=S16LE", sampleRate), "!",
			"identity", fmt.Sprintf("ts-offset=%d", offset), "!",
			"queue", "!",
			"interleave.",
		)
	}

	// Add interleave and output chain
	args = append(args,
		"interleave", "name=interleave", "!",
		"audioconvert", "!",
		"audioresample", "!",
		fmt.Sprintf("audio/x-raw,rate=%d,channels=%d,format=S16LE", encodeSampleRate, len(participantOrder)), "!",
	)

	// Add encoder based on format
	switch format {
	case types.AudioRecordingFormatOGGOpus:
		args = append(args, "opusenc", "!", "oggmux", "!")
	case types.AudioRecordingFormatWAVPCM:
		args = append(args, "wavenc", "!")
	}

	args = append(args, "filesink", fmt.Sprintf("location=%s", outputPath))

	// Run gst-launch-1.0
	cmd := exec.CommandContext(ctx, "gst-launch-1.0", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Debugw("running merge pipeline", "args", args, "participants", participantOrder, "channels", len(participantOrder))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gst-launch failed: %w", err)
	}

	return nil
}

func orderedParticipantIDs(participantFiles map[string]string, alignment *AlignmentResult) []string {
	ordered := make([]string, 0, len(participantFiles))
	seen := make(map[string]struct{}, len(participantFiles))

	if alignment != nil {
		for _, a := range alignment.Alignments {
			if _, ok := participantFiles[a.ParticipantID]; !ok {
				continue
			}
			if _, ok := seen[a.ParticipantID]; ok {
				continue
			}
			ordered = append(ordered, a.ParticipantID)
			seen[a.ParticipantID] = struct{}{}
		}
	}

	remaining := make([]string, 0, len(participantFiles)-len(ordered))
	for participantID := range participantFiles {
		if _, ok := seen[participantID]; ok {
			continue
		}
		remaining = append(remaining, participantID)
	}
	sort.Strings(remaining)

	return append(ordered, remaining...)
}

func nearestOpusRate(sampleRate int32) int32 {
	validRates := []int32{8000, 12000, 16000, 24000, 48000}
	nearest := validRates[0]
	minDiff := absInt32(sampleRate - nearest)

	for _, r := range validRates[1:] {
		diff := absInt32(sampleRate - r)
		if diff < minDiff {
			minDiff = diff
			nearest = r
		}
	}

	return nearest
}

func absInt32(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

// uploadMergedFiles uploads the merged files to storage
func (w *MergeWorker) uploadMergedFiles(_ context.Context, manifest *config.AudioRecordingManifest, mergedFiles map[types.AudioRecordingFormat]string, manifestPath string) error {
	manifest.InitRoomMix()
	remoteManifestPath := w.normalizeRemotePath(manifestPath)

	for format, localPath := range mergedFiles {
		// Calculate checksum
		checksum, size, err := w.calculateChecksum(localPath)
		if err != nil {
			return err
		}

		// Build storage path
		mixedFilename := mixedAudioFilename(manifest.RoomName, format)
		storagePath := path.Join(path.Dir(remoteManifestPath), mixedFilename)

		// Upload or copy to final location
		location := storagePath
		if w.storage != nil {
			uploadedLocation, _, err := w.storage.UploadFile(localPath, storagePath, string(config.GetOutputTypeForFormat(format)))
			if err != nil {
				return err
			}
			location = uploadedLocation
		} else {
			// Local storage: copy merged file next to manifest
			destPath := path.Join(path.Dir(manifestPath), mixedFilename)
			if err := copyFile(localPath, destPath); err != nil {
				return fmt.Errorf("failed to copy merged file: %w", err)
			}
			location = destPath
		}

		// Create artifact
		artifact := &config.AudioArtifact{
			Format:     format,
			Filename:   path.Base(location),
			StorageURI: location,
			Size:       size,
			SHA256:     checksum,
			UploadedAt: time.Now().UnixNano(),
		}

		manifest.AddRoomMixArtifact(artifact)
	}

	manifest.SetRoomMixStatus(config.AudioRecordingStatusCompleted, "")
	return nil
}

func mixedAudioFilename(roomName string, format types.AudioRecordingFormat) string {
	return fmt.Sprintf("room_mix_%s%s", sanitizeFilenameComponent(roomName), config.GetFileExtensionForFormat(format))
}

func sanitizeFilenameComponent(s string) string {
	if s == "" {
		return "room"
	}

	var b strings.Builder
	b.Grow(len(s))

	prevUnderscore := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '-' || r == '_' {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}

		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}

	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "room"
	}
	return out
}

// calculateChecksum calculates SHA-256 checksum of a file
func (w *MergeWorker) calculateChecksum(filepath string) (string, int64, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// updateManifestWithMergeResults updates and re-uploads the manifest
func (w *MergeWorker) updateManifestWithMergeResults(_ context.Context, manifestPath string, manifest *config.AudioRecordingManifest) error {
	data, err := manifest.ToJSON()
	if err != nil {
		return err
	}

	if w.hasRemoteStorage() {
		if err := w.ensureStorage(); err != nil {
			return err
		}

		// Write to temp file and upload
		tmpPath := path.Join(w.config.TmpDir, "manifest_updated.json")
		if err := os.WriteFile(tmpPath, data, 0644); err != nil {
			return err
		}
		defer os.Remove(tmpPath)

		remoteManifestPath := w.normalizeRemotePath(manifestPath)
		uploadManifestPath := remoteManifestPath
		// GCS updates can require storage.objects.delete on overwrite.
		// To avoid that requirement, write a sibling merged manifest object.
		if w.config.StorageConfig != nil && w.config.StorageConfig.GCP != nil {
			uploadManifestPath = mergedManifestObjectKey(remoteManifestPath)
		}

		_, _, err = w.storage.UploadFile(tmpPath, uploadManifestPath, string(types.OutputTypeJSON))
		if err == nil && uploadManifestPath != remoteManifestPath {
			logger.Infow("uploaded merged manifest to a new key",
				"originalManifestPath", remoteManifestPath,
				"mergedManifestPath", uploadManifestPath,
			)
		}
		return err
	}

	// Local storage: write directly to the manifest path
	return os.WriteFile(manifestPath, data, 0644)
}

func mergedManifestObjectKey(manifestPath string) string {
	base := path.Base(manifestPath)
	if base == "." || base == "/" || base == "" {
		return manifestPath
	}

	ext := path.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		return manifestPath
	}

	mergedName := name + "_merged" + ext
	return path.Join(path.Dir(manifestPath), mergedName)
}

func (w *MergeWorker) hasRemoteStorage() bool {
	return w != nil && w.config != nil && w.config.StorageConfig != nil && !w.config.StorageConfig.IsLocal()
}

func (w *MergeWorker) ensureStorage() error {
	if w.storage != nil || !w.hasRemoteStorage() {
		return nil
	}

	store, err := getStorage(w.config.StorageConfig)
	if err != nil {
		return err
	}

	w.storage = store
	return nil
}

func (w *MergeWorker) normalizeRemotePath(remotePath string) string {
	if !w.hasRemoteStorage() {
		return remotePath
	}

	if w.config.StorageConfig.S3 != nil {
		return normalizeS3ObjectKey(w.config.StorageConfig.S3.Bucket, remotePath)
	}
	if w.config.StorageConfig.GCP != nil {
		return normalizeGCSObjectKey(w.config.StorageConfig.GCP.Bucket, remotePath)
	}

	return remotePath
}

func normalizeS3ObjectKey(bucket, remotePath string) string {
	u, err := url.Parse(remotePath)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return remotePath
	}

	pathPart := strings.TrimPrefix(u.Path, "/")
	if pathPart == "" {
		return remotePath
	}

	key := pathPart
	if bucket != "" {
		bucketPrefix := bucket + "/"
		if strings.HasPrefix(pathPart, bucketPrefix) {
			remainder := strings.TrimPrefix(pathPart, bucketPrefix)
			if strings.HasPrefix(remainder, "/") {
				key = "/" + strings.TrimPrefix(remainder, "/")
			} else {
				key = remainder
			}
		} else if strings.HasPrefix(strings.ToLower(u.Hostname()), strings.ToLower(bucket)+".") {
			key = pathPart
		}
	}

	if decodedKey, err := url.PathUnescape(key); err == nil {
		return decodedKey
	}

	return key
}

func normalizeGCSObjectKey(bucket, remotePath string) string {
	u, err := url.Parse(remotePath)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return remotePath
	}

	pathPart := strings.TrimPrefix(u.Path, "/")
	if pathPart == "" {
		return remotePath
	}

	key := pathPart
	if bucket != "" {
		bucketPrefix := bucket + "/"
		if strings.HasPrefix(pathPart, bucketPrefix) {
			remainder := strings.TrimPrefix(pathPart, bucketPrefix)
			if strings.HasPrefix(remainder, "/") {
				key = "/" + strings.TrimPrefix(remainder, "/")
			} else {
				key = remainder
			}
		} else if strings.EqualFold(u.Hostname(), bucket) || strings.HasPrefix(strings.ToLower(u.Hostname()), strings.ToLower(bucket)+".") {
			key = pathPart
		}
	}

	if decodedKey, err := url.PathUnescape(key); err == nil {
		return decodedKey
	}

	return key
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	if err := os.MkdirAll(path.Dir(dst), 0755); err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
