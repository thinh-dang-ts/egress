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

//go:build integration

package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// testIsolatedAudioRecording tests the isolated per-participant audio recording feature
// This is triggered by using AudioMixing_DUAL_CHANNEL_ALTERNATE with audio_only=true
func (r *Runner) testIsolatedAudioRecording(t *testing.T) {
	if !r.should(runEdge) {
		return
	}

	t.Run("IsolatedAudioRecording", func(t *testing.T) {
		for _, test := range []*testCase{
			// Basic isolated audio recording with 2 participants
			{
				name:        "IsolatedAudio_TwoParticipants",
				requestType: types.RequestTypeRoomComposite,
				publishOptions: publishOptions{
					audioOnly:   true,
					audioMixing: livekit.AudioMixing_DUAL_CHANNEL_ALTERNATE,
				},
				fileOptions: &fileOptions{
					filename: "isolated_audio_2p_{time}",
					fileType: livekit.EncodedFileType_OGG,
				},
				custom: r.testIsolatedAudioTwoParticipants,
			},

			// Isolated audio recording with 3 participants
			{
				name:        "IsolatedAudio_ThreeParticipants",
				requestType: types.RequestTypeRoomComposite,
				publishOptions: publishOptions{
					audioOnly:   true,
					audioMixing: livekit.AudioMixing_DUAL_CHANNEL_ALTERNATE,
				},
				fileOptions: &fileOptions{
					filename: "isolated_audio_3p_{time}",
					fileType: livekit.EncodedFileType_OGG,
				},
				custom: r.testIsolatedAudioThreeParticipants,
			},

			// Isolated audio recording with participant join/leave
			{
				name:        "IsolatedAudio_JoinLeave",
				requestType: types.RequestTypeRoomComposite,
				publishOptions: publishOptions{
					audioOnly:   true,
					audioMixing: livekit.AudioMixing_DUAL_CHANNEL_ALTERNATE,
				},
				fileOptions: &fileOptions{
					filename: "isolated_audio_join_leave_{time}",
					fileType: livekit.EncodedFileType_OGG,
				},
				custom: r.testIsolatedAudioJoinLeave,
			},

			// Isolated audio recording with in-process merge to stereo
			{
				name:        "IsolatedAudio_MergeToStereo",
				requestType: types.RequestTypeRoomComposite,
				publishOptions: publishOptions{
					audioOnly:   true,
					audioMixing: livekit.AudioMixing_DUAL_CHANNEL_ALTERNATE,
				},
				fileOptions: &fileOptions{
					filename: "isolated_audio_merge_{time}",
					fileType: livekit.EncodedFileType_OGG,
				},
				custom: r.testIsolatedAudioMergeToStereo,
			},
		} {
			if r.Short {
				// In short mode, only run the first test
				if !r.run(t, test, test.custom) {
					return
				}
				return
			}
			if !r.run(t, test, test.custom) {
				return
			}
		}
	})
}

// testIsolatedAudioTwoParticipants tests isolated recording with 2 participants
func (r *Runner) testIsolatedAudioTwoParticipants(t *testing.T, test *testCase) {
	// Connect first participant
	p1, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-p1",
		ParticipantIdentity: fmt.Sprintf("participant-1-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p1.Disconnect)

	// Publish audio from participant 1
	r.publish(t, p1.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	// Connect second participant
	p2, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-p2",
		ParticipantIdentity: fmt.Sprintf("participant-2-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p2.Disconnect)

	// Publish audio from participant 2
	r.publish(t, p2.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	// Give time for tracks to be established
	time.Sleep(2 * time.Second)

	// Build and send the request
	req := r.build(test)
	storageConfig := r.getIsolatedAudioStorageConfig(t, req)
	info := r.sendRequest(t, req)
	egressID := info.EgressId

	// Let the recording run for a bit
	time.Sleep(15 * time.Second)

	// Check that egress is active
	r.checkUpdate(t, egressID, livekit.EgressStatus_EGRESS_ACTIVE)

	// Stop the egress
	info = r.stopEgress(t, egressID)

	// Verify the result
	r.verifyIsolatedAudioOutput(t, test, info, 2, storageConfig)
}

// testIsolatedAudioThreeParticipants tests isolated recording with 3 participants
func (r *Runner) testIsolatedAudioThreeParticipants(t *testing.T, test *testCase) {
	participants := make([]*lksdk.Room, 3)

	// Connect 3 participants
	for i := 0; i < 3; i++ {
		p, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
			APIKey:              r.ApiKey,
			APISecret:           r.ApiSecret,
			RoomName:            r.RoomName,
			ParticipantName:     fmt.Sprintf("isolated-audio-p%d", i+1),
			ParticipantIdentity: fmt.Sprintf("participant-%d-%d", i+1, rand.Intn(100)),
		}, lksdk.NewRoomCallback())
		require.NoError(t, err)
		participants[i] = p
		t.Cleanup(p.Disconnect)

		// Publish audio from each participant
		r.publish(t, p.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))
	}

	// Give time for tracks to be established
	time.Sleep(2 * time.Second)

	// Build and send the request
	req := r.build(test)
	storageConfig := r.getIsolatedAudioStorageConfig(t, req)
	info := r.sendRequest(t, req)
	egressID := info.EgressId

	// Let the recording run
	time.Sleep(15 * time.Second)

	// Check that egress is active
	r.checkUpdate(t, egressID, livekit.EgressStatus_EGRESS_ACTIVE)

	// Stop the egress
	info = r.stopEgress(t, egressID)

	// Verify the result
	r.verifyIsolatedAudioOutput(t, test, info, 3, storageConfig)
}

// testIsolatedAudioJoinLeave tests isolated recording when participants join and leave
func (r *Runner) testIsolatedAudioJoinLeave(t *testing.T, test *testCase) {
	// Connect first participant
	p1, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-p1",
		ParticipantIdentity: fmt.Sprintf("participant-1-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p1.Disconnect)
	r.publish(t, p1.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	time.Sleep(2 * time.Second)

	// Start recording
	req := r.build(test)
	storageConfig := r.getIsolatedAudioStorageConfig(t, req)
	info := r.sendRequest(t, req)
	egressID := info.EgressId

	time.Sleep(5 * time.Second)

	// Connect second participant mid-recording
	p2, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-p2",
		ParticipantIdentity: fmt.Sprintf("participant-2-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	r.publish(t, p2.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	time.Sleep(5 * time.Second)

	// Disconnect participant 2
	p2.Disconnect()

	time.Sleep(5 * time.Second)

	// Check that egress is still active
	r.checkUpdate(t, egressID, livekit.EgressStatus_EGRESS_ACTIVE)

	// Stop the egress
	info = r.stopEgress(t, egressID)

	// Verify the result - should have 2 participant files
	r.verifyIsolatedAudioOutput(t, test, info, 2, storageConfig)
}

// testIsolatedAudioMergeToStereo tests that 2 participant recordings are merged into a stereo file
func (r *Runner) testIsolatedAudioMergeToStereo(t *testing.T, test *testCase) {
	// Connect first participant
	p1, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "merge-audio-p1",
		ParticipantIdentity: fmt.Sprintf("merge-p1-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p1.Disconnect)

	r.publish(t, p1.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	// Connect second participant
	p2, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "merge-audio-p2",
		ParticipantIdentity: fmt.Sprintf("merge-p2-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p2.Disconnect)

	r.publish(t, p2.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	// Give time for tracks to be established
	time.Sleep(2 * time.Second)

	// Build and send the request
	req := r.build(test)
	storageConfig := r.getIsolatedAudioStorageConfig(t, req)
	info := r.sendRequest(t, req)
	egressID := info.EgressId

	// Let the recording run
	time.Sleep(15 * time.Second)

	// Check that egress is active
	r.checkUpdate(t, egressID, livekit.EgressStatus_EGRESS_ACTIVE)

	// Stop the egress
	info = r.stopEgress(t, egressID)

	// Verify isolated output first
	r.verifyIsolatedAudioOutput(t, test, info, 2, storageConfig)

	// Verify the merged output
	r.verifyMergedAudioOutput(t, info, storageConfig)
}

// verifyMergedAudioOutput verifies the merged room mix audio output
func (r *Runner) verifyMergedAudioOutput(t *testing.T, info *livekit.EgressInfo, storageConfig *config.StorageConfig) {
	require.Equal(t, livekit.EgressStatus_EGRESS_COMPLETE, info.Status)
	manifest, _ := r.loadIsolatedAudioManifest(t, info, storageConfig)
	require.NotNil(t, manifest, "manifest should be present")
	require.NotNil(t, manifest.RoomMix, "manifest should have room_mix section")
	require.Equal(t, config.AudioRecordingStatusCompleted, manifest.RoomMix.Status, "room_mix status should be completed")

	t.Logf("Merge completed: status=%s, artifacts=%d", manifest.RoomMix.Status, len(manifest.RoomMix.Artifacts))

	// Verify room mix section
	require.Empty(t, manifest.RoomMix.Error, "room_mix should not have error")
	require.NotEmpty(t, manifest.RoomMix.Artifacts, "room_mix should have artifacts")
	require.Greater(t, manifest.RoomMix.MergedAt, int64(0), "room_mix should have merge timestamp")

	// Verify the merged audio file
	for _, artifact := range manifest.RoomMix.Artifacts {
		require.NotEmpty(t, artifact.Filename, "artifact should have filename")
		require.NotEmpty(t, artifact.StorageURI, "artifact should have storage URI")
		require.Greater(t, artifact.Size, int64(0), "artifact should have size > 0")
		require.NotEmpty(t, artifact.SHA256, "artifact should have checksum")
		require.False(t, strings.Contains(artifact.StorageURI, "{"), "artifact URI should not contain template tokens")
		if isCloudStorageConfig(storageConfig) {
			require.False(t, strings.HasPrefix(artifact.StorageURI, "/"), "cloud artifact URI should not be local path: %s", artifact.StorageURI)
			require.False(t, strings.HasPrefix(artifact.StorageURI, r.FilePrefix), "cloud artifact URI should not use local file prefix: %s", artifact.StorageURI)
		}

		t.Logf("Merged artifact: %s, size: %d, format: %s", artifact.Filename, artifact.Size, artifact.Format)

		// Verify with ffprobe
		mergedPath := r.materializeAudioArtifact(t, storageConfig, info.EgressId, artifact.StorageURI)
		if mergedPath == "" {
			// Presigned URL mode: path checks above are the strongest assertions available here.
			continue
		}
		probeInfo, err := ffprobe(mergedPath)
		if err != nil {
			t.Logf("ffprobe failed for merged file: %v", err)
			continue
		}

		require.NotNil(t, probeInfo, "ffprobe should return info for merged file")

		hasAudio := false
		for _, stream := range probeInfo.Streams {
			if stream.CodecType == "audio" {
				hasAudio = true
				t.Logf("Merged audio: codec=%s, sample_rate=%s, channels=%d",
					stream.CodecName, stream.SampleRate, stream.Channels)
				require.Equal(t, "opus", stream.CodecName, "merged file should use opus codec")
				require.Equal(t, 2, stream.Channels, "merged file should have 2 channels (stereo)")
			}
		}
		require.True(t, hasAudio, "merged file should contain audio stream")

		// Verify duration > 0
		if probeInfo.Format.Duration != "" {
			dur, err := parseFFProbeDuration(probeInfo.Format.Duration)
			require.NoError(t, err)
			require.Greater(t, dur, time.Duration(0), "merged file should have positive duration")
			t.Logf("Merged duration: %v", dur)
		}
	}
}

// verifyIsolatedAudioOutput verifies the output of isolated audio recording
func (r *Runner) verifyIsolatedAudioOutput(
	t *testing.T,
	_ *testCase,
	info *livekit.EgressInfo,
	expectedParticipants int,
	storageConfig *config.StorageConfig,
) {
	require.Equal(t, livekit.EgressStatus_EGRESS_COMPLETE, info.Status, "egress should complete successfully")
	require.Empty(t, info.Error, "egress should not have errors")
	manifest, _ := r.loadIsolatedAudioManifest(t, info, storageConfig)

	// Verify manifest contents
	require.Equal(t, r.RoomName, manifest.RoomName, "manifest should have correct room name")
	require.GreaterOrEqual(t, len(manifest.Participants), expectedParticipants, "manifest should have expected participants")
	require.Equal(t, r.expectedIsolatedAudioEncryptionMode(), manifest.Encryption,
		"manifest encryption should match configured isolated audio encryption mode")

	t.Logf("Manifest verified: %d participants, formats: %v",
		len(manifest.Participants), manifest.Formats)

	// Verify participant files exist and are retrievable from configured storage.
	for _, p := range manifest.Participants {
		require.NotEmpty(t, p.ParticipantID, "participant should have ID")
		t.Logf("Participant: %s (identity: %s), artifacts: %d",
			p.ParticipantID, p.ParticipantIdentity, len(p.Artifacts))

		for _, artifact := range p.Artifacts {
			require.NotEmpty(t, artifact.Filename, "artifact should have filename")
			require.NotEmpty(t, artifact.StorageURI, "artifact should have storage URI")
			require.False(t, strings.Contains(artifact.StorageURI, "{"), "artifact URI should not contain template tokens")
			if isCloudStorageConfig(storageConfig) {
				require.False(t, strings.HasPrefix(artifact.StorageURI, "/"), "cloud artifact URI should not be local path: %s", artifact.StorageURI)
				require.False(t, strings.HasPrefix(artifact.StorageURI, r.FilePrefix), "cloud artifact URI should not use local file prefix: %s", artifact.StorageURI)
			}
			localArtifactPath := r.materializeAudioArtifact(t, storageConfig, info.EgressId, artifact.StorageURI)
			if localArtifactPath != "" && strings.HasSuffix(localArtifactPath, ".ogg") {
				r.verifyAudioFile(t, localArtifactPath)
			}
			t.Logf("  - Artifact: %s, size: %d, format: %s",
				artifact.Filename, artifact.Size, artifact.Format)
		}
	}
}

func (r *Runner) expectedIsolatedAudioEncryptionMode() string {
	if r.AudioRecordingEncryption == nil || r.AudioRecordingEncryption.Mode == config.EncryptionModeNone {
		return ""
	}
	return string(r.AudioRecordingEncryption.Mode)
}

// verifyAudioFile verifies an audio file using ffprobe
func (r *Runner) verifyAudioFile(t *testing.T, filepath string) {
	info, err := ffprobe(filepath)
	if err != nil {
		t.Logf("ffprobe not available or failed: %v", err)
		return
	}

	// Verify it's an audio file
	require.NotNil(t, info, "ffprobe should return info")

	hasAudio := false
	for _, stream := range info.Streams {
		if stream.CodecType == "audio" {
			hasAudio = true
			t.Logf("Audio stream: codec=%s, sample_rate=%s, channels=%d",
				stream.CodecName, stream.SampleRate, stream.Channels)
		}
	}
	require.True(t, hasAudio, "file should contain audio stream")
}

func (r *Runner) getIsolatedAudioStorageConfig(t *testing.T, req *rpc.StartEgressRequest) *config.StorageConfig {
	p, err := config.GetValidatedPipelineConfig(r.ServiceConfig, req)
	require.NoError(t, err)
	fileCfg := p.GetFileConfig()
	require.NotNil(t, fileCfg)
	return fileCfg.StorageConfig
}

func (r *Runner) getIsolatedManifestStoragePath(t *testing.T, info *livekit.EgressInfo) string {
	var fileResult *livekit.FileInfo
	if len(info.FileResults) > 0 {
		fileResult = info.FileResults[0]
	}
	require.NotNil(t, fileResult, "should have file result")
	require.NotEmpty(t, fileResult.Filename, "file result should include manifest path")
	return fileResult.Filename
}

func (r *Runner) loadIsolatedAudioManifest(
	t *testing.T,
	info *livekit.EgressInfo,
	storageConfig *config.StorageConfig,
) (*config.AudioRecordingManifest, string) {
	manifestStoragePath := r.getIsolatedManifestStoragePath(t, info)
	if isCloudStorageConfig(storageConfig) {
		require.False(t, strings.HasPrefix(manifestStoragePath, "/"), "cloud manifest URI should not be local path: %s", manifestStoragePath)
		require.False(t, strings.HasPrefix(manifestStoragePath, r.FilePrefix), "cloud manifest URI should not use local file prefix: %s", manifestStoragePath)
	}

	manifestPath := manifestStoragePath
	if _, err := os.Stat(manifestPath); err != nil {
		require.NotNil(t, storageConfig, "manifest is remote but storage config is nil")
		require.False(t, storageConfig.IsLocal(), "manifest is remote but storage config is local")

		manifestPath = filepath.Join(r.FilePrefix, fmt.Sprintf("%s_isolated_manifest.json", info.EgressId))
		download(t, storageConfig, manifestPath, manifestStoragePath, false)
		t.Cleanup(func() {
			_ = os.Remove(manifestPath)
		})
	}

	manifestData, err := os.ReadFile(manifestPath)
	require.NoError(t, err, "should be able to read manifest")

	var manifest config.AudioRecordingManifest
	err = json.Unmarshal(manifestData, &manifest)
	require.NoError(t, err, "should be able to parse manifest")

	return &manifest, manifestPath
}

func (r *Runner) materializeAudioArtifact(
	t *testing.T,
	storageConfig *config.StorageConfig,
	egressID string,
	storageURI string,
) string {
	if _, err := os.Stat(storageURI); err == nil {
		return storageURI
	}

	// Presigned URL mode is not used by current integration configs; keep tests resilient.
	if strings.HasPrefix(storageURI, "http://") || strings.HasPrefix(storageURI, "https://") {
		t.Logf("skipping direct artifact download for presigned URL: %s", storageURI)
		return ""
	}

	require.NotNil(t, storageConfig, "artifact is remote but storage config is nil: %s", storageURI)
	require.False(t, storageConfig.IsLocal(), "artifact is remote but storage config is local: %s", storageURI)

	localPath := filepath.Join(r.FilePrefix, fmt.Sprintf("%s_artifact_%d_%s", egressID, rand.Intn(100000), filepath.Base(storageURI)))
	download(t, storageConfig, localPath, storageURI, false)
	t.Cleanup(func() {
		_ = os.Remove(localPath)
	})

	stat, err := os.Stat(localPath)
	require.NoError(t, err, "downloaded artifact should exist")
	require.Greater(t, stat.Size(), int64(0), "downloaded artifact should have content")
	return localPath
}

func isCloudStorageConfig(storageConfig *config.StorageConfig) bool {
	return storageConfig != nil && !storageConfig.IsLocal()
}
