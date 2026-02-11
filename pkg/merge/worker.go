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
	"os"
	"os/exec"
	"path"
	"time"

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
	)

	return nil
}

// downloadManifest downloads and parses the manifest file
func (w *MergeWorker) downloadManifest(ctx context.Context, manifestPath string, tmpDir string) (*config.AudioRecordingManifest, error) {
	readPath := manifestPath
	if w.storage != nil {
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

		if w.storage != nil {
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
	if w.storage == nil {
		// Local storage - file should already exist or we can't download
		return nil
	}

	_, err := w.storage.DownloadFile(remotePath, localPath)
	return err
}

// mergeTracks merges participant tracks using GStreamer
func (w *MergeWorker) mergeTracks(ctx context.Context, manifest *config.AudioRecordingManifest, participantFiles map[string]string, alignment *AlignmentResult, tmpDir string) (map[types.AudioRecordingFormat]string, error) {
	mergedFiles := make(map[types.AudioRecordingFormat]string)

	// Build GStreamer pipeline for merging
	// Pipeline: filesrc -> decoder -> identity(ts-offset) -> audiomixer -> encoder -> filesink
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
	// For each participant: filesrc ! decodebin ! audioconvert ! audioresample ! identity ts-offset=X ! audiomixer.
	// audiomixer ! audioconvert ! audioresample ! encoder ! filesink

	args := []string{"-e"} // Send EOS on interrupt
	encodeSampleRate := sampleRate
	if format == types.AudioRecordingFormatOGGOpus {
		encodeSampleRate = nearestOpusRate(sampleRate)
	}

	// Add mixer input for each participant
	for participantID, filePath := range participantFiles {
		// Find alignment for this participant
		var offset int64
		for _, a := range alignment.Alignments {
			if a.ParticipantID == participantID {
				offset = a.GetIdentityTsOffset()
				break
			}
		}

		// Add source chain for this participant
		args = append(args,
			"filesrc", fmt.Sprintf("location=%s", filePath), "!",
			"decodebin", "!",
			"audioconvert", "!",
			"audioresample", "!",
			fmt.Sprintf("audio/x-raw,rate=%d,channels=2", sampleRate), "!",
			"identity", fmt.Sprintf("ts-offset=%d", offset), "!",
			"audiomixer.",
		)
	}

	// Add mixer and output chain
	args = append(args,
		"audiomixer", "name=audiomixer", "!",
		"audioconvert", "!",
		"audioresample", "!",
		fmt.Sprintf("audio/x-raw,rate=%d,channels=2,format=S16LE", encodeSampleRate), "!",
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

	logger.Debugw("running merge pipeline", "args", args)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gst-launch failed: %w", err)
	}

	return nil
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

	for format, localPath := range mergedFiles {
		// Calculate checksum
		checksum, size, err := w.calculateChecksum(localPath)
		if err != nil {
			return err
		}

		// Build storage path
		storagePath := fmt.Sprintf("%s/%s/room_mix%s",
			manifest.RoomName,
			manifest.SessionID,
			config.GetFileExtensionForFormat(format),
		)

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
			destPath := path.Join(path.Dir(manifestPath), fmt.Sprintf("room_mix%s", config.GetFileExtensionForFormat(format)))
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

	if w.storage != nil {
		// Write to temp file and upload
		tmpPath := path.Join(w.config.TmpDir, "manifest_updated.json")
		if err := os.WriteFile(tmpPath, data, 0644); err != nil {
			return err
		}
		defer os.Remove(tmpPath)

		_, _, err = w.storage.UploadFile(tmpPath, manifestPath, string(types.OutputTypeJSON))
		return err
	}

	// Local storage: write directly to the manifest path
	return os.WriteFile(manifestPath, data, 0644)
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
