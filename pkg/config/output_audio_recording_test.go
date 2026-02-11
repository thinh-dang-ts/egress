package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
)

func TestCreateAudioRecordingConfigFromFileOutput_UsesConfiguredEncryption(t *testing.T) {
	p := &PipelineConfig{
		BaseConfig: BaseConfig{
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
	require.Equal(t, "", ar.AudioManifest.Encryption)
}
