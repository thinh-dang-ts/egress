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

package sink

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"
	"time"

	"github.com/linkdata/deadlock"
	livekit "github.com/livekit/protocol/livekit"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/gstreamer"
	"github.com/livekit/egress/pkg/pipeline/sink/uploader"
	"github.com/livekit/egress/pkg/stats"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
)

// AudioRecordingSink manages per-participant audio recording sinks
type AudioRecordingSink struct {
	*base

	conf     *config.PipelineConfig
	arConf   *config.AudioRecordingConfig
	uploader *uploader.Uploader
	monitor  *stats.HandlerMonitor
	fileInfo *livekit.FileInfo // populated on close to report size/location

	mu              deadlock.RWMutex
	participantSinks map[string]*ParticipantAudioSink

	// Merge job queue client (will be injected)
	mergeJobEnqueuer MergeJobEnqueuer

	closed bool
}

// ParticipantAudioSink holds the sink for a single participant
type ParticipantAudioSink struct {
	ParticipantID       string
	ParticipantIdentity string
	TrackID             string
	Config              *config.ParticipantAudioConfig
	Artifacts           []*config.AudioArtifact
	Closed              bool
}

// MergeJobEnqueuer interface for enqueuing merge jobs
type MergeJobEnqueuer interface {
	EnqueueMergeJob(manifestPath string, sessionID string) error
}

// newAudioRecordingSink creates a new audio recording sink
func newAudioRecordingSink(
	_ *gstreamer.Pipeline,
	conf *config.PipelineConfig,
	o *config.AudioRecordingConfig,
	monitor *stats.HandlerMonitor,
) (*AudioRecordingSink, error) {
	u, err := uploader.New(o.StorageConfig, conf.BackupConfig, monitor, conf.StorageReporter, conf.Info)
	if err != nil {
		return nil, err
	}

	sink := &AudioRecordingSink{
		base:             &base{},
		conf:             conf,
		arConf:           o,
		uploader:         u,
		monitor:          monitor,
		participantSinks: make(map[string]*ParticipantAudioSink),
	}

	// Initialize manifest
	encryptionMode := ""
	if o.IsEncryptionEnabled() {
		encryptionMode = string(o.Encryption.Mode)
	}
	o.AudioManifest = config.NewAudioRecordingManifest(
		conf.Info.EgressId,
		conf.Info.RoomId,
		conf.Info.RoomName,
		o.SessionID,
		o.Formats,
		o.SampleRate,
		encryptionMode,
	)

	return sink, nil
}

// Start initializes the audio recording sink
func (s *AudioRecordingSink) Start() error {
	logger.Debugw("audio recording sink started",
		"sessionID", s.arConf.SessionID,
		"formats", s.arConf.Formats,
		"sampleRate", s.arConf.SampleRate,
	)
	return nil
}

// AddParticipant adds a new participant to the recording
func (s *AudioRecordingSink) AddParticipant(participantID, participantIdentity, trackID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.participantSinks[participantID]; exists {
		logger.Warnw("participant already exists", nil, "participantID", participantID)
		return nil
	}

	// Add participant to config
	pConfig := s.arConf.AddParticipant(participantID, participantIdentity, trackID)

	// Add to manifest
	s.arConf.AudioManifest.AddParticipant(participantID, participantIdentity, trackID)

	sink := &ParticipantAudioSink{
		ParticipantID:       participantID,
		ParticipantIdentity: participantIdentity,
		TrackID:             trackID,
		Config:              pConfig,
		Artifacts:           make([]*config.AudioArtifact, 0),
	}

	s.participantSinks[participantID] = sink

	logger.Infow("participant added to audio recording",
		"participantID", participantID,
		"participantIdentity", participantIdentity,
		"trackID", trackID,
	)

	return nil
}

// RemoveParticipant removes a participant from the recording
func (s *AudioRecordingSink) RemoveParticipant(participantID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sink, exists := s.participantSinks[participantID]
	if !exists {
		return nil
	}

	if sink.Closed {
		return nil
	}

	// Mark participant as left in config and manifest
	s.arConf.RemoveParticipant(participantID)
	s.arConf.AudioManifest.UpdateParticipantLeft(participantID)

	// Upload files for this participant
	if err := s.uploadParticipantFiles(sink); err != nil {
		logger.Errorw("failed to upload participant files", err, "participantID", participantID)
		return err
	}

	sink.Closed = true

	logger.Infow("participant removed from audio recording",
		"participantID", participantID,
		"artifacts", len(sink.Artifacts),
	)

	return nil
}

// SetParticipantClockSync sets clock sync info for a participant
func (s *AudioRecordingSink) SetParticipantClockSync(participantID string, clockSync *config.ClockSyncInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sink, exists := s.participantSinks[participantID]
	if !exists {
		return
	}

	sink.Config.ClockSyncInfo = clockSync
	s.arConf.AudioManifest.SetParticipantClockSync(participantID, clockSync)

	logger.Debugw("clock sync info set",
		"participantID", participantID,
		"serverTimestamp", clockSync.ServerTimestamp,
		"rtpClockBase", clockSync.RTPClockBase,
	)
}

// uploadParticipantFiles uploads all files for a participant (via ParticipantAudioSink)
func (s *AudioRecordingSink) uploadParticipantFiles(sink *ParticipantAudioSink) error {
	uploaded, err := s.uploadParticipantConfigFiles(sink.ParticipantID, sink.Config)
	if err != nil {
		return err
	}
	// Record total uploaded size as a single artifact for tracking
	if uploaded > 0 {
		sink.Artifacts = append(sink.Artifacts, &config.AudioArtifact{Size: uploaded})
	}
	return nil
}

// uploadParticipantConfigFiles uploads all files for a participant using config directly.
// Returns the total uploaded size.
func (s *AudioRecordingSink) uploadParticipantConfigFiles(participantID string, pConfig *config.ParticipantAudioConfig) (int64, error) {
	var totalUploaded int64
	for format, localPath := range pConfig.LocalFilepaths {
		storagePath := pConfig.StorageFilepaths[format]

		// Calculate checksum before upload
		checksum, size, err := s.calculateFileChecksum(localPath)
		if err != nil {
			logger.Warnw("failed to calculate checksum", err, "path", localPath)
			continue
		}

		// Upload file
		start := time.Now()
		location, uploadedSize, err := s.uploader.Upload(localPath, storagePath, config.GetOutputTypeForFormat(format), false)
		if err != nil {
			logger.Errorw("failed to upload file", err, "path", localPath)
			return totalUploaded, err
		}

		// Get file duration (approximate based on size for WAV, or use metadata)
		durationMs := s.estimateDurationMs(format, size)

		artifact := &config.AudioArtifact{
			Format:     format,
			Filename:   path.Base(storagePath),
			StorageURI: location,
			Size:       uploadedSize,
			DurationMs: durationMs,
			SHA256:     checksum,
			UploadedAt: time.Now().UnixNano(),
		}

		s.arConf.AudioManifest.AddParticipantArtifact(participantID, artifact)
		totalUploaded += uploadedSize

		logger.Debugw("participant file uploaded",
			"participantID", participantID,
			"format", format,
			"size", uploadedSize,
			"duration", time.Since(start),
		)
	}

	return totalUploaded, nil
}

// calculateFileChecksum calculates SHA-256 checksum of a file
func (s *AudioRecordingSink) calculateFileChecksum(filepath string) (string, int64, error) {
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

// estimateDurationMs estimates audio duration in milliseconds
func (s *AudioRecordingSink) estimateDurationMs(format types.AudioRecordingFormat, size int64) int64 {
	switch format {
	case types.AudioRecordingFormatWAVPCM:
		// WAV PCM: 16-bit stereo at sample rate
		// bytes per second = sample_rate * channels * bytes_per_sample
		// = sample_rate * 2 * 2 = sample_rate * 4
		bytesPerSecond := int64(s.arConf.SampleRate) * 4
		if bytesPerSecond > 0 {
			// Subtract WAV header (~44 bytes)
			dataSize := size - 44
			if dataSize < 0 {
				dataSize = 0
			}
			return (dataSize * 1000) / bytesPerSecond
		}
	case types.AudioRecordingFormatOGGOpus:
		// For Opus, duration estimation from size is less accurate
		// Assume average bitrate based on sample rate
		bitrate := s.getBitrateForSampleRate() * 1000 // Convert to bps
		if bitrate > 0 {
			return (size * 8 * 1000) / int64(bitrate)
		}
	}
	return 0
}

// getBitrateForSampleRate returns approximate bitrate for sample rate
func (s *AudioRecordingSink) getBitrateForSampleRate() int32 {
	switch s.arConf.SampleRate {
	case 8000:
		return 24
	case 16000:
		return 32
	case 24000:
		return 48
	case 32000:
		return 64
	case 44100, 48000:
		return 96
	default:
		return 64
	}
}

// AddEOSProbe signals EOS received immediately since AudioRecordingSink
// manages its own finalization in Close() without a GStreamer bin.
func (s *AudioRecordingSink) AddEOSProbe() {
	s.eosReceived.Store(true)
}

// EOSReceived returns whether EOS has been received (interface requirement)
func (s *AudioRecordingSink) EOSReceived() bool {
	return s.eosReceived.Load()
}

// UploadManifest uploads the manifest file
func (s *AudioRecordingSink) UploadManifest(filepath string) (string, bool, error) {
	storagePath := s.arConf.BuildMixedFilepath(types.AudioRecordingFormatOGGOpus)
	storagePath = path.Join(path.Dir(storagePath), "manifest.json")

	location, _, err := s.uploader.Upload(filepath, storagePath, types.OutputTypeJSON, false)
	if err != nil {
		return "", false, err
	}

	return location, true, nil
}

// Close finalizes the audio recording
func (s *AudioRecordingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	logger.Infow("closing audio recording sink",
		"sessionID", s.arConf.SessionID,
		"participants", len(s.arConf.ParticipantConfigs),
	)

	// Upload participant files. Participants may have been registered directly via
	// AddParticipant (stored in s.participantSinks) or by the AudioRecordingBin
	// calling arConf.AddParticipant (stored only in s.arConf.ParticipantConfigs).
	// We iterate the config's participant map to cover both cases.
	var totalSize int64
	for participantID, pConfig := range s.arConf.ParticipantConfigs {
		// Skip participants already uploaded via RemoveParticipant
		if pSink, ok := s.participantSinks[participantID]; ok && pSink.Closed {
			// Already uploaded; accumulate size from artifacts
			for _, a := range pSink.Artifacts {
				totalSize += a.Size
			}
			continue
		}

		// Ensure participant exists in manifest (may have been added by bin only)
		if s.arConf.AudioManifest.GetParticipant(participantID) == nil {
			s.arConf.AudioManifest.AddParticipant(
				participantID,
				pConfig.ParticipantIdentity,
				pConfig.TrackID,
			)
		}

		s.arConf.RemoveParticipant(participantID)
		s.arConf.AudioManifest.UpdateParticipantLeft(participantID)

		uploaded, err := s.uploadParticipantConfigFiles(participantID, pConfig)
		if err != nil {
			logger.Errorw("failed to upload participant files on close", err, "participantID", participantID)
		}
		totalSize += uploaded
	}

	// Update manifest status
	s.arConf.AudioManifest.SetStatus(config.AudioRecordingStatusUploading)

	// Write and upload manifest
	manifestData, err := s.arConf.AudioManifest.Close(time.Now().UnixNano())
	if err != nil {
		logger.Errorw("failed to close manifest", err)
		return err
	}

	// Write manifest to temp file
	manifestPath := path.Join(s.arConf.LocalDir, "manifest.json")
	if err := os.WriteFile(manifestPath, manifestData, 0644); err != nil {
		logger.Errorw("failed to write manifest file", err)
		return err
	}

	// Upload manifest
	manifestLocation, _, err := s.UploadManifest(manifestPath)
	if err != nil {
		logger.Errorw("failed to upload manifest", err)
		return err
	}

	logger.Infow("manifest uploaded", "location", manifestLocation)

	// Update FileInfo so the egress result reports correct size/location
	if s.fileInfo != nil {
		s.fileInfo.Filename = manifestPath
		s.fileInfo.Location = manifestLocation
		s.fileInfo.Size = totalSize
		logger.Debugw("fileInfo updated",
			"filename", s.fileInfo.Filename,
			"size", s.fileInfo.Size,
		)
	}

	// Enqueue merge job if FinalRoomMix is enabled
	if s.arConf.HasFinalRoomMix() && s.mergeJobEnqueuer != nil {
		if err := s.mergeJobEnqueuer.EnqueueMergeJob(manifestLocation, s.arConf.SessionID); err != nil {
			logger.Errorw("failed to enqueue merge job", err)
			// Don't return error - recording is complete, merge is optional
		} else {
			logger.Infow("merge job enqueued", "sessionID", s.arConf.SessionID)
		}
	}

	return nil
}

// SetMergeJobEnqueuer sets the merge job enqueuer
func (s *AudioRecordingSink) SetMergeJobEnqueuer(enqueuer MergeJobEnqueuer) {
	s.mergeJobEnqueuer = enqueuer
}

// GetManifest returns the current manifest
func (s *AudioRecordingSink) GetManifest() *config.AudioRecordingManifest {
	return s.arConf.AudioManifest
}

// GetParticipantCount returns the number of participants
func (s *AudioRecordingSink) GetParticipantCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.participantSinks)
}

// GetActiveParticipants returns IDs of active participants
func (s *AudioRecordingSink) GetActiveParticipants() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := make([]string, 0)
	for id, sink := range s.participantSinks {
		if !sink.Closed {
			active = append(active, id)
		}
	}
	return active
}
