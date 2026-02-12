package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
)

func TestCreateAudioRecordingConfigFromFileOutput_UsesConfiguredEncryption(t *testing.T) {
	p := &PipelineConfig{
		BaseConfig: BaseConfig{
			AudioRecordingPathPrefix: "{room_name}/{session_id}",
			AudioRecordingEncryption: &EncryptionConfig{
				Mode:      EncryptionModeAES,
				MasterKey: "test-master-key",
			},
		},
		TmpDir: t.TempDir(),
		Info: &livekit.EgressInfo{
			EgressId: "egress-1",
			RoomId:   "room-id-1",
			RoomName: "test-room",
		},
		AudioConfig: AudioConfig{
			AudioFrequency: 48000,
		},
		Outputs: map[types.EgressType][]OutputConfig{
			types.EgressTypeFile: {
				&FileConfig{
					outputConfig:  outputConfig{OutputType: types.OutputTypeOGG},
					LocalFilepath: "/tmp/test.ogg",
				},
			},
		},
	}

	ar := p.createAudioRecordingConfigFromFileOutput()
	require.NotNil(t, ar)
	require.NotNil(t, ar.Encryption)
	require.True(t, ar.IsEncryptionEnabled())
	require.Equal(t, EncryptionModeAES, ar.Encryption.Mode)
	require.Equal(t, "test-master-key", ar.Encryption.MasterKey)
	require.Equal(t, "{room_name}/{session_id}", ar.PathPrefix)
	require.Equal(t, string(EncryptionModeAES), ar.AudioManifest.Encryption)

	require.NotSame(t, p.AudioRecordingEncryption, ar.Encryption)
	p.AudioRecordingEncryption.Mode = EncryptionModeNone
	require.Equal(t, EncryptionModeAES, ar.Encryption.Mode)
}

func TestCreateAudioRecordingConfigFromFileOutput_NoEncryptionConfigured(t *testing.T) {
	p := &PipelineConfig{
		TmpDir: t.TempDir(),
		Info: &livekit.EgressInfo{
			EgressId: "egress-1",
			RoomId:   "room-id-1",
			RoomName: "test-room",
		},
		AudioConfig: AudioConfig{
			AudioFrequency: 48000,
		},
		Outputs: map[types.EgressType][]OutputConfig{
			types.EgressTypeFile: {
				&FileConfig{
					outputConfig:  outputConfig{OutputType: types.OutputTypeOGG},
					LocalFilepath: "/tmp/test.ogg",
				},
			},
		},
	}

	ar := p.createAudioRecordingConfigFromFileOutput()
	require.NotNil(t, ar)
	require.Nil(t, ar.Encryption)
	require.False(t, ar.IsEncryptionEnabled())
	require.Equal(t, "", ar.PathPrefix)
	require.Equal(t, "", ar.AudioManifest.Encryption)
}

func TestCreateAudioRecordingConfigFromFileOutput_PathPrefixAppliedToParticipantPath(t *testing.T) {
	p := &PipelineConfig{
		BaseConfig: BaseConfig{
			AudioRecordingPathPrefix: "prefix/{room_name}/{session_id}",
		},
		TmpDir: t.TempDir(),
		Info: &livekit.EgressInfo{
			EgressId: "egress-1",
			RoomId:   "room-id-1",
			RoomName: "test-room",
		},
		AudioConfig: AudioConfig{
			AudioFrequency: 48000,
		},
		Outputs: map[types.EgressType][]OutputConfig{
			types.EgressTypeFile: {
				&FileConfig{
					outputConfig:  outputConfig{OutputType: types.OutputTypeOGG},
					LocalFilepath: "/tmp/test.ogg",
				},
			},
		},
	}

	ar := p.createAudioRecordingConfigFromFileOutput()
	require.NotNil(t, ar)
	require.Equal(t, "prefix/{room_name}/{session_id}", ar.PathPrefix)

	pc := ar.AddParticipant("participant-1", "alice", "track-1")
	storagePath := pc.StorageFilepaths[types.AudioRecordingFormatOGGOpus]
	require.NotEmpty(t, storagePath)
	require.True(t, strings.HasPrefix(storagePath, "prefix/test-room/egress-1/"), "unexpected storage path: %q", storagePath)
	require.True(t, strings.HasSuffix(storagePath, ".ogg"), "unexpected storage path: %q", storagePath)
	require.NotContains(t, storagePath, "{room_name}")
	require.NotContains(t, storagePath, "{session_id}")
}
