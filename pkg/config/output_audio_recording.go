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

package config

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/types"
)

// EncryptionMode specifies the type of encryption to use
type EncryptionMode string

const (
	EncryptionModeNone   EncryptionMode = "none"
	EncryptionModeS3SSE  EncryptionMode = "s3_sse"   // S3 Server-Side Encryption (SSE-S3)
	EncryptionModeS3KMS  EncryptionMode = "s3_kms"   // S3 SSE with KMS (SSE-KMS)
	EncryptionModeGCSCMK EncryptionMode = "gcs_cmek" // GCS Customer-Managed Encryption Key
	EncryptionModeAES    EncryptionMode = "aes_256"  // AES-256-GCM envelope encryption for local storage
)

// EncryptionConfig specifies encryption settings for audio recordings
type EncryptionConfig struct {
	Mode        EncryptionMode `yaml:"mode" json:"mode"`
	KMSKeyID    string         `yaml:"kms_key_id,omitempty" json:"kms_key_id,omitempty"`       // For S3 SSE-KMS
	CMEKKeyName string         `yaml:"cmek_key_name,omitempty" json:"cmek_key_name,omitempty"` // For GCS CMEK
	MasterKey   string         `yaml:"master_key,omitempty" json:"master_key,omitempty"`       // For local AES encryption (base64 encoded)
}

// AudioRecordingConfig contains configuration for audio-only recording
type AudioRecordingConfig struct {
	outputConfig

	// Room and session identification
	RoomName  string `yaml:"room_name" json:"room_name"`
	SessionID string `yaml:"session_id" json:"session_id"`

	// Recording options
	IsolatedTracks bool                         `yaml:"isolated_tracks" json:"isolated_tracks"` // Record each participant separately (default: true)
	FinalRoomMix   bool                         `yaml:"final_room_mix" json:"final_room_mix"`   // Generate offline-mixed room audio (default: false)
	Formats        []types.AudioRecordingFormat `yaml:"formats" json:"formats"`                 // Output formats (ogg_opus, wav_pcm)
	SampleRate     int32                        `yaml:"sample_rate" json:"sample_rate"`         // 8000, 16000, 24000, 32000, 44100, 48000

	// Encryption settings
	Encryption *EncryptionConfig `yaml:"encryption,omitempty" json:"encryption,omitempty"`

	// Storage settings
	StorageConfig *StorageConfig `yaml:"storage,omitempty" json:"storage,omitempty"`
	PathPrefix    string         `yaml:"path_prefix" json:"path_prefix"` // e.g., "{env}/{room_id}/{session_id}"

	// Local paths (generated)
	LocalDir string `yaml:"-" json:"-"`

	// Participant track configs (populated at runtime)
	ParticipantConfigs map[string]*ParticipantAudioConfig `yaml:"-" json:"-"`

	// Manifest for this recording session
	AudioManifest *AudioRecordingManifest `yaml:"-" json:"-"`
}

// ParticipantAudioConfig holds per-participant recording configuration
type ParticipantAudioConfig struct {
	ParticipantID       string                                `json:"participant_id"`
	ParticipantIdentity string                                `json:"participant_identity"`
	TrackID             string                                `json:"track_id"`
	LocalFilepaths      map[types.AudioRecordingFormat]string `json:"-"` // Format -> local filepath
	StorageFilepaths    map[types.AudioRecordingFormat]string `json:"-"` // Format -> storage filepath
	JoinedAt            int64                                 `json:"joined_at"`
	LeftAt              int64                                 `json:"left_at,omitempty"`
	ClockSyncInfo       *ClockSyncInfo                        `json:"clock_sync_info,omitempty"`
}

// ClockSyncInfo stores timing information for offline alignment
type ClockSyncInfo struct {
	ServerTimestamp int64  `json:"server_timestamp"` // Server time when first packet received (ns)
	RTPClockBase    uint32 `json:"rtp_clock_base"`   // First RTP timestamp
	ClockRate       uint32 `json:"clock_rate"`       // RTP clock rate (e.g., 48000)
}

// NewAudioRecordingConfig creates a new AudioRecordingConfig with defaults
func NewAudioRecordingConfig() *AudioRecordingConfig {
	return &AudioRecordingConfig{
		outputConfig:       outputConfig{OutputType: types.OutputTypeOGG},
		IsolatedTracks:     true,
		FinalRoomMix:       false,
		Formats:            []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus},
		SampleRate:         types.DefaultAudioRecordingSampleRate,
		ParticipantConfigs: make(map[string]*ParticipantAudioConfig),
	}
}

// Validate validates the AudioRecordingConfig
func (c *AudioRecordingConfig) Validate() error {
	if c.RoomName == "" {
		return errors.ErrInvalidInput("room_name")
	}

	if c.SessionID == "" {
		return errors.ErrInvalidInput("session_id")
	}

	// Validate formats
	if len(c.Formats) == 0 {
		return errors.ErrInvalidInput("formats")
	}
	for _, format := range c.Formats {
		if format != types.AudioRecordingFormatOGGOpus && format != types.AudioRecordingFormatWAVPCM {
			return errors.ErrInvalidInput(fmt.Sprintf("format: %s", format))
		}
	}

	// Validate sample rate
	if !slices.Contains(types.ValidAudioRecordingSampleRates, c.SampleRate) {
		return errors.ErrInvalidInput(fmt.Sprintf("sample_rate: %d", c.SampleRate))
	}

	// Validate encryption config
	return c.validateEncryption()
}

// validateEncryption validates encryption settings
func (c *AudioRecordingConfig) validateEncryption() error {
	if c.Encryption == nil || c.Encryption.Mode == EncryptionModeNone {
		return nil
	}

	switch c.Encryption.Mode {
	case EncryptionModeS3SSE:
		// No additional parameters needed for SSE-S3
		return nil

	case EncryptionModeS3KMS:
		if c.Encryption.KMSKeyID == "" {
			return errors.ErrInvalidInput("encryption.kms_key_id required for S3 KMS encryption")
		}
		if c.StorageConfig == nil || c.StorageConfig.S3 == nil {
			return errors.ErrInvalidInput("S3 storage required for S3 KMS encryption")
		}
		return nil

	case EncryptionModeGCSCMK:
		if c.Encryption.CMEKKeyName == "" {
			return errors.ErrInvalidInput("encryption.cmek_key_name required for GCS CMEK encryption")
		}
		if c.StorageConfig == nil || c.StorageConfig.GCP == nil {
			return errors.ErrInvalidInput("GCS storage required for GCS CMEK encryption")
		}
		return nil

	case EncryptionModeAES:
		if c.Encryption.MasterKey == "" {
			return errors.ErrInvalidInput("encryption.master_key required for AES encryption")
		}
		return nil

	default:
		return errors.ErrInvalidInput(fmt.Sprintf("encryption.mode: %s", c.Encryption.Mode))
	}
}

// GetOutputTypeForFormat returns the OutputType for a given AudioRecordingFormat
func GetOutputTypeForFormat(format types.AudioRecordingFormat) types.OutputType {
	switch format {
	case types.AudioRecordingFormatOGGOpus:
		return types.OutputTypeOGG
	case types.AudioRecordingFormatWAVPCM:
		return types.OutputTypeWAV
	default:
		return types.OutputTypeOGG
	}
}

// GetFileExtensionForFormat returns the file extension for a given AudioRecordingFormat
func GetFileExtensionForFormat(format types.AudioRecordingFormat) types.FileExtension {
	switch format {
	case types.AudioRecordingFormatOGGOpus:
		return types.FileExtensionOGG
	case types.AudioRecordingFormatWAVPCM:
		return types.FileExtensionWAV
	default:
		return types.FileExtensionOGG
	}
}

// BuildParticipantFilepath builds the storage filepath for a participant recording
func (c *AudioRecordingConfig) BuildParticipantFilepath(participantID, participantIdentity string, format types.AudioRecordingFormat) string {
	ext := GetFileExtensionForFormat(format)
	timestamp := time.Now().Format("20060102T150405")

	// Build path: {prefix}/{participant_identity}_{participant_id}/{timestamp}.{ext}
	filename := fmt.Sprintf("%s_%s_%s%s", participantIdentity, participantID, timestamp, ext)

	prefix := c.buildStoragePrefix()
	return path.Join(prefix, filename)
}

// BuildMixedFilepath builds the storage filepath for the mixed room audio
func (c *AudioRecordingConfig) BuildMixedFilepath(format types.AudioRecordingFormat) string {
	ext := GetFileExtensionForFormat(format)
	timestamp := time.Now().Format("20060102T150405")

	filename := fmt.Sprintf("room_mix_%s%s", timestamp, ext)
	prefix := c.buildStoragePrefix()
	return path.Join(prefix, filename)
}

// buildStoragePrefix builds the storage prefix with template substitution
func (c *AudioRecordingConfig) buildStoragePrefix() string {
	prefix := c.PathPrefix
	if prefix == "" {
		prefix = "{room_name}/{session_id}"
	}

	// Replace template variables
	replacements := map[string]string{
		"{room_name}":  c.RoomName,
		"{room_id}":    c.RoomName, // Room ID and name are often the same
		"{session_id}": c.SessionID,
		"{time}":       time.Now().Format("2006-01-02T150405"),
		"{utc}":        fmt.Sprintf("%d", time.Now().Unix()),
	}

	for template, value := range replacements {
		prefix = strings.Replace(prefix, template, value, -1)
	}

	return prefix
}

// AddParticipant adds a new participant to the recording
func (c *AudioRecordingConfig) AddParticipant(participantID, participantIdentity, trackID string) *ParticipantAudioConfig {
	config := &ParticipantAudioConfig{
		ParticipantID:       participantID,
		ParticipantIdentity: participantIdentity,
		TrackID:             trackID,
		LocalFilepaths:      make(map[types.AudioRecordingFormat]string),
		StorageFilepaths:    make(map[types.AudioRecordingFormat]string),
		JoinedAt:            time.Now().UnixNano(),
	}

	// Generate file paths for each format
	for _, format := range c.Formats {
		storageFilepath := c.BuildParticipantFilepath(participantID, participantIdentity, format)
		config.StorageFilepaths[format] = storageFilepath

		_, filename := path.Split(storageFilepath)
		config.LocalFilepaths[format] = path.Join(c.LocalDir, filename)
	}

	c.ParticipantConfigs[participantID] = config
	return config
}

// RemoveParticipant marks a participant as having left the recording
func (c *AudioRecordingConfig) RemoveParticipant(participantID string) {
	if config, ok := c.ParticipantConfigs[participantID]; ok {
		config.LeftAt = time.Now().UnixNano()
	}
}

// GetParticipantConfig returns the config for a specific participant
func (c *AudioRecordingConfig) GetParticipantConfig(participantID string) *ParticipantAudioConfig {
	return c.ParticipantConfigs[participantID]
}

// GetAudioRecordingConfig returns the AudioRecordingConfig from pipeline config
// If in isolated audio recording mode (RoomComposite + DUAL_CHANNEL_ALTERNATE),
// it creates the config from FileOutput configuration
func (p *PipelineConfig) GetAudioRecordingConfig() *AudioRecordingConfig {
	// First check for explicit AudioRecording output
	o, ok := p.Outputs[types.EgressTypeAudioRecording]
	if ok && len(o) > 0 {
		return o[0].(*AudioRecordingConfig)
	}

	// In isolated audio recording mode, create config from FileOutput (cached)
	if p.IsolatedAudioRecording {
		if p.cachedAudioRecordingCfg == nil {
			p.cachedAudioRecordingCfg = p.createAudioRecordingConfigFromFileOutput()
		}
		return p.cachedAudioRecordingCfg
	}

	return nil
}

// createAudioRecordingConfigFromFileOutput creates an AudioRecordingConfig
// from the existing FileOutput configuration for isolated recording mode
func (p *PipelineConfig) createAudioRecordingConfigFromFileOutput() *AudioRecordingConfig {
	fileConfig := p.GetFileConfig()
	if fileConfig == nil {
		return nil
	}

	// Determine format from file output type
	var formats []types.AudioRecordingFormat
	switch fileConfig.OutputType {
	case types.OutputTypeOGG:
		formats = []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus}
	case types.OutputTypeWAV:
		formats = []types.AudioRecordingFormat{types.AudioRecordingFormatWAVPCM}
	default:
		// Default to OGG for audio recording
		formats = []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus}
	}

	// Use audio frequency from config, default to 48000
	sampleRate := p.AudioFrequency
	if sampleRate == 0 {
		sampleRate = 48000
	}

	// Use TmpDir for local directory, or derive from file path
	localDir := p.TmpDir
	if localDir == "" && fileConfig.LocalFilepath != "" {
		localDir = path.Dir(fileConfig.LocalFilepath)
	}

	arConfig := &AudioRecordingConfig{
		RoomName:           p.Info.RoomName,
		SessionID:          p.Info.EgressId,
		IsolatedTracks:     true,
		FinalRoomMix:       p.FinalRoomMix,
		Formats:            formats,
		SampleRate:         sampleRate,
		Encryption:         copyEncryptionConfig(p.AudioRecordingEncryption),
		PathPrefix:         p.AudioRecordingPathPrefix,
		LocalDir:           localDir,
		ParticipantConfigs: make(map[string]*ParticipantAudioConfig),
	}

	encryptionMode := ""
	if arConfig.IsEncryptionEnabled() {
		encryptionMode = string(arConfig.Encryption.Mode)
	}

	// Initialize manifest
	arConfig.AudioManifest = NewAudioRecordingManifest(
		p.Info.EgressId,     // egressID
		p.Info.RoomId,       // roomID
		p.Info.RoomName,     // roomName
		p.Info.EgressId,     // sessionID (use egressID as session)
		arConfig.Formats,    // formats
		arConfig.SampleRate, // sampleRate
		encryptionMode,
	)

	return arConfig
}

// HasIsolatedTracks returns true if isolated track recording is enabled
func (c *AudioRecordingConfig) HasIsolatedTracks() bool {
	return c.IsolatedTracks
}

// HasFinalRoomMix returns true if final room mix generation is enabled
func (c *AudioRecordingConfig) HasFinalRoomMix() bool {
	return c.FinalRoomMix
}

// SupportsFormat returns true if the config supports the given format
func (c *AudioRecordingConfig) SupportsFormat(format types.AudioRecordingFormat) bool {
	return slices.Contains(c.Formats, format)
}

// IsEncryptionEnabled returns true if encryption is enabled
func (c *AudioRecordingConfig) IsEncryptionEnabled() bool {
	return c.Encryption != nil && c.Encryption.Mode != EncryptionModeNone
}

func copyEncryptionConfig(in *EncryptionConfig) *EncryptionConfig {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
