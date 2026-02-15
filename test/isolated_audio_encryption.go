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
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/encryption"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func (r *Runner) testIsolatedAudioEncryption(t *testing.T) {
	if !r.should(runEdge) {
		return
	}

	t.Run("IsolatedAudioEncryption", func(t *testing.T) {
		key, err := encryption.GenerateMasterKeyBase64()
		require.NoError(t, err)

		originalEncryption := r.AudioRecordingEncryption
		originalFinalRoomMix := r.FinalRoomMix
		r.AudioRecordingEncryption = &config.EncryptionConfig{
			Mode:      config.EncryptionModeAES,
			MasterKey: key,
		}
		r.FinalRoomMix = true
		t.Cleanup(func() {
			r.AudioRecordingEncryption = originalEncryption
			r.FinalRoomMix = originalFinalRoomMix
		})

		test := &testCase{
			name:        "IsolatedAudio_AES_MergeEncrypted",
			requestType: types.RequestTypeRoomComposite,
			publishOptions: publishOptions{
				audioOnly:   true,
				audioMixing: livekit.AudioMixing_DUAL_CHANNEL_ALTERNATE,
			},
			fileOptions: &fileOptions{
				filename: "isolated_audio_aes_{time}",
				fileType: livekit.EncodedFileType_OGG,
			},
			custom: r.testIsolatedAudioAESEncryption,
		}

		r.run(t, test, test.custom)
	})
}

func (r *Runner) testIsolatedAudioAESEncryption(t *testing.T, test *testCase) {
	p1, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-aes-p1",
		ParticipantIdentity: fmt.Sprintf("participant-aes-1-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p1.Disconnect)
	r.publish(t, p1.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	p2, err := lksdk.ConnectToRoom(r.WsUrl, lksdk.ConnectInfo{
		APIKey:              r.ApiKey,
		APISecret:           r.ApiSecret,
		RoomName:            r.RoomName,
		ParticipantName:     "isolated-audio-aes-p2",
		ParticipantIdentity: fmt.Sprintf("participant-aes-2-%d", rand.Intn(100)),
	}, lksdk.NewRoomCallback())
	require.NoError(t, err)
	t.Cleanup(p2.Disconnect)
	r.publish(t, p2.LocalParticipant, types.MimeTypeOpus, make(chan struct{}))

	time.Sleep(2 * time.Second)

	req := r.build(test)
	storageConfig := r.getIsolatedAudioStorageConfig(t, req)
	info := r.sendRequest(t, req)
	egressID := info.EgressId

	time.Sleep(15 * time.Second)

	r.checkUpdate(t, egressID, livekit.EgressStatus_EGRESS_ACTIVE)
	info = r.stopEgress(t, egressID)

	r.verifyIsolatedAudioOutput(t, test, info, 2, storageConfig)
	r.verifyMergedAudioOutput(t, info, storageConfig, 2)

	manifest, _ := r.loadIsolatedAudioManifest(t, info, storageConfig)
	r.verifyAESArtifactsEncrypted(t, info, storageConfig, manifest)
}

func (r *Runner) verifyAESArtifactsEncrypted(
	t *testing.T,
	info *livekit.EgressInfo,
	storageConfig *config.StorageConfig,
	manifest *config.AudioRecordingManifest,
) {
	require.NotNil(t, r.AudioRecordingEncryption)
	require.Equal(t, config.EncryptionModeAES, r.AudioRecordingEncryption.Mode)
	require.Equal(t, string(config.EncryptionModeAES), manifest.Encryption)

	encryptor, err := encryption.NewEnvelopeEncryptorFromBase64(r.AudioRecordingEncryption.MasterKey)
	require.NoError(t, err)

	participantArtifactCount := 0
	for _, participant := range manifest.Participants {
		for _, artifact := range participant.Artifacts {
			participantArtifactCount++
			r.verifyArtifactIsEncryptedAudio(t, storageConfig, info.EgressId, artifact, encryptor)
		}
	}
	require.Greater(t, participantArtifactCount, 0, "expected participant artifacts")

	require.NotNil(t, manifest.RoomMix)
	require.NotEmpty(t, manifest.RoomMix.Artifacts)
	for _, artifact := range manifest.RoomMix.Artifacts {
		r.verifyArtifactIsEncryptedAudio(t, storageConfig, info.EgressId, artifact, encryptor)
	}
}

func (r *Runner) verifyArtifactIsEncryptedAudio(
	t *testing.T,
	storageConfig *config.StorageConfig,
	egressID string,
	artifact *config.AudioArtifact,
	encryptor *encryption.EnvelopeEncryptor,
) {
	t.Helper()
	require.NotNil(t, artifact)

	artifactPath := r.materializeAudioArtifact(t, storageConfig, egressID, artifact.StorageURI)
	if artifactPath == "" {
		return
	}

	f, err := os.Open(artifactPath)
	require.NoError(t, err)
	defer f.Close()

	header := make([]byte, len(encryption.HeaderMagic))
	_, err = io.ReadFull(f, header)
	require.NoError(t, err)
	require.Equal(t, encryption.HeaderMagic, string(header), "artifact should be AES envelope encrypted")

	ext := filepath.Ext(artifact.Filename)
	decryptedPath := filepath.Join(
		r.FilePrefix,
		fmt.Sprintf("%s_decrypted_%d%s", egressID, rand.Intn(100000), ext),
	)
	require.NoError(t, decryptAudioArtifact(artifactPath, decryptedPath, encryptor))
	t.Cleanup(func() {
		_ = os.Remove(decryptedPath)
	})

	stat, err := os.Stat(decryptedPath)
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(0), "decrypted artifact should not be empty")

	r.verifyAudioFile(t, decryptedPath)
}

func decryptAudioArtifact(srcPath, dstPath string, encryptor *encryption.EnvelopeEncryptor) error {
	reader, err := encryption.NewEncryptedTempFileReader(srcPath, encryptor)
	if err != nil {
		return err
	}
	defer reader.Close()

	output, err := os.Create(dstPath)
	if err != nil {
		return err
	}

	if _, err = io.Copy(output, reader); err != nil {
		_ = output.Close()
		_ = os.Remove(dstPath)
		return err
	}

	if err = output.Close(); err != nil {
		_ = os.Remove(dstPath)
		return err
	}

	return nil
}
