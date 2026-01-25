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
	"bytes"
	"encoding/json"
	"time"

	"github.com/linkdata/deadlock"

	"github.com/livekit/egress/pkg/types"
)

// AudioRecordingStatus represents the status of an audio recording
type AudioRecordingStatus string

const (
	AudioRecordingStatusRecording  AudioRecordingStatus = "recording"
	AudioRecordingStatusUploading  AudioRecordingStatus = "uploading"
	AudioRecordingStatusMerging    AudioRecordingStatus = "merging"
	AudioRecordingStatusCompleted  AudioRecordingStatus = "completed"
	AudioRecordingStatusFailed     AudioRecordingStatus = "failed"
)

// AudioRecordingManifest contains metadata for an audio recording session
type AudioRecordingManifest struct {
	mu deadlock.RWMutex

	// Session identification
	EgressID  string `json:"egress_id"`
	RoomID    string `json:"room_id"`
	RoomName  string `json:"room_name"`
	SessionID string `json:"session_id"`

	// Recording parameters
	Formats     []types.AudioRecordingFormat `json:"formats"`
	SampleRate  int32                        `json:"sample_rate"`
	ChannelCount int32                       `json:"channel_count"`
	Encryption  string                       `json:"encryption,omitempty"` // Encryption mode used

	// Timing
	StartedAt int64 `json:"started_at"` // Unix nanoseconds
	EndedAt   int64 `json:"ended_at,omitempty"`

	// Participant recordings
	Participants []*ParticipantRecordingInfo `json:"participants"`

	// Mixed room audio (if FinalRoomMix enabled)
	RoomMix *RoomMixInfo `json:"room_mix,omitempty"`

	// Status and error
	Status AudioRecordingStatus `json:"status"`
	Error  string               `json:"error,omitempty"`
}

// ParticipantRecordingInfo contains recording info for a single participant
type ParticipantRecordingInfo struct {
	ParticipantID       string `json:"participant_id"`
	ParticipantIdentity string `json:"participant_identity"`
	TrackID             string `json:"track_id"`

	// Timing
	JoinedAt int64 `json:"joined_at"` // Unix nanoseconds
	LeftAt   int64 `json:"left_at,omitempty"`

	// Clock sync for offline alignment
	ClockSync *ClockSyncInfo `json:"clock_sync,omitempty"`

	// Artifact files
	Artifacts []*AudioArtifact `json:"artifacts"`
}

// RoomMixInfo contains info about the mixed room audio
type RoomMixInfo struct {
	Status    AudioRecordingStatus `json:"status"`
	Artifacts []*AudioArtifact     `json:"artifacts"`
	MergedAt  int64                `json:"merged_at,omitempty"`
	Error     string               `json:"error,omitempty"`
}

// AudioArtifact represents a single audio file artifact
type AudioArtifact struct {
	Format       types.AudioRecordingFormat `json:"format"`
	Filename     string                     `json:"filename"`
	StorageURI   string                     `json:"storage_uri"`
	Size         int64                      `json:"size"`
	DurationMs   int64                      `json:"duration_ms"`
	SHA256       string                     `json:"sha256"`
	UploadedAt   int64                      `json:"uploaded_at,omitempty"`
	PresignedURL string                     `json:"presigned_url,omitempty"`
}

// NewAudioRecordingManifest creates a new AudioRecordingManifest
func NewAudioRecordingManifest(egressID, roomID, roomName, sessionID string, formats []types.AudioRecordingFormat, sampleRate int32, encryption string) *AudioRecordingManifest {
	return &AudioRecordingManifest{
		EgressID:     egressID,
		RoomID:       roomID,
		RoomName:     roomName,
		SessionID:    sessionID,
		Formats:      formats,
		SampleRate:   sampleRate,
		ChannelCount: 2, // Stereo
		Encryption:   encryption,
		StartedAt:    time.Now().UnixNano(),
		Participants: make([]*ParticipantRecordingInfo, 0),
		Status:       AudioRecordingStatusRecording,
	}
}

// AddParticipant adds a new participant to the manifest
func (m *AudioRecordingManifest) AddParticipant(participantID, participantIdentity, trackID string) *ParticipantRecordingInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	info := &ParticipantRecordingInfo{
		ParticipantID:       participantID,
		ParticipantIdentity: participantIdentity,
		TrackID:             trackID,
		JoinedAt:            time.Now().UnixNano(),
		Artifacts:           make([]*AudioArtifact, 0),
	}

	m.Participants = append(m.Participants, info)
	return info
}

// GetParticipant returns the participant info for a given participant ID
func (m *AudioRecordingManifest) GetParticipant(participantID string) *ParticipantRecordingInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, p := range m.Participants {
		if p.ParticipantID == participantID {
			return p
		}
	}
	return nil
}

// UpdateParticipantLeft marks a participant as having left
func (m *AudioRecordingManifest) UpdateParticipantLeft(participantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range m.Participants {
		if p.ParticipantID == participantID {
			p.LeftAt = time.Now().UnixNano()
			return
		}
	}
}

// SetParticipantClockSync sets the clock sync info for a participant
func (m *AudioRecordingManifest) SetParticipantClockSync(participantID string, clockSync *ClockSyncInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range m.Participants {
		if p.ParticipantID == participantID {
			p.ClockSync = clockSync
			return
		}
	}
}

// AddParticipantArtifact adds an artifact to a participant's recording
func (m *AudioRecordingManifest) AddParticipantArtifact(participantID string, artifact *AudioArtifact) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range m.Participants {
		if p.ParticipantID == participantID {
			p.Artifacts = append(p.Artifacts, artifact)
			return
		}
	}
}

// SetStatus sets the overall recording status
func (m *AudioRecordingManifest) SetStatus(status AudioRecordingStatus) {
	m.mu.Lock()
	m.Status = status
	m.mu.Unlock()
}

// SetError sets the error message
func (m *AudioRecordingManifest) SetError(err string) {
	m.mu.Lock()
	m.Error = err
	m.Status = AudioRecordingStatusFailed
	m.mu.Unlock()
}

// InitRoomMix initializes the room mix info
func (m *AudioRecordingManifest) InitRoomMix() {
	m.mu.Lock()
	m.RoomMix = &RoomMixInfo{
		Status:    AudioRecordingStatusMerging,
		Artifacts: make([]*AudioArtifact, 0),
	}
	m.mu.Unlock()
}

// AddRoomMixArtifact adds an artifact to the room mix
func (m *AudioRecordingManifest) AddRoomMixArtifact(artifact *AudioArtifact) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.RoomMix != nil {
		m.RoomMix.Artifacts = append(m.RoomMix.Artifacts, artifact)
	}
}

// SetRoomMixStatus sets the room mix status
func (m *AudioRecordingManifest) SetRoomMixStatus(status AudioRecordingStatus, err string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.RoomMix != nil {
		m.RoomMix.Status = status
		m.RoomMix.Error = err
		if status == AudioRecordingStatusCompleted {
			m.RoomMix.MergedAt = time.Now().UnixNano()
		}
	}
}

// Close finalizes the manifest
func (m *AudioRecordingManifest) Close(endedAt int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.EndedAt = endedAt
	if m.Status == AudioRecordingStatusRecording || m.Status == AudioRecordingStatusUploading {
		m.Status = AudioRecordingStatusCompleted
	}

	return m.marshalLocked()
}

// ToJSON converts the manifest to JSON
func (m *AudioRecordingManifest) ToJSON() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.marshalLocked()
}

func (m *AudioRecordingManifest) marshalLocked() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GetParticipantCount returns the number of participants
func (m *AudioRecordingManifest) GetParticipantCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Participants)
}

// GetActiveParticipants returns participants who haven't left yet
func (m *AudioRecordingManifest) GetActiveParticipants() []*ParticipantRecordingInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	active := make([]*ParticipantRecordingInfo, 0)
	for _, p := range m.Participants {
		if p.LeftAt == 0 {
			active = append(active, p)
		}
	}
	return active
}

// GetAllParticipantArtifacts returns all artifacts from all participants
func (m *AudioRecordingManifest) GetAllParticipantArtifacts() []*AudioArtifact {
	m.mu.RLock()
	defer m.mu.RUnlock()

	artifacts := make([]*AudioArtifact, 0)
	for _, p := range m.Participants {
		artifacts = append(artifacts, p.Artifacts...)
	}
	return artifacts
}
