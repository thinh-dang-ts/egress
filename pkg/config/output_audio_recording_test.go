package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
)

func TestCreateAudioRecordingConfigFromFileOutput_UsesConfiguredEncryption(t *testing.T) {
	storageCfg := &StorageConfig{Prefix: "request-prefix"}

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
					StorageConfig: storageCfg,
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
	require.Same(t, storageCfg, ar.StorageConfig)
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

// TestBuildMixedFilepath_DirectFilepath_Empty verifies fallback to PathPrefix when no
// explicit filepath is provided.
func TestBuildMixedFilepath_DirectFilepath_Empty(t *testing.T) {
	c := &AudioRecordingConfig{
		RoomName:   "my-room",
		SessionID:  "sess-1",
		PathPrefix: "{room_name}/{session_id}",
		Formats:    []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus},
	}

	mixed := c.BuildMixedFilepath(types.AudioRecordingFormatOGGOpus)
	require.Contains(t, mixed, "my-room/sess-1/room_mix_")
	require.True(t, strings.HasSuffix(mixed, ".ogg"), "mixed path: %q", mixed)

	prefix := c.buildStoragePrefix()
	require.Equal(t, "my-room/sess-1", prefix)
}

// TestBuildMixedFilepath_DirectFilepath_WithExtension verifies the exact-path behaviour
// when DirectFilepath ends with a known audio extension.
func TestBuildMixedFilepath_DirectFilepath_WithExtension(t *testing.T) {
	c := &AudioRecordingConfig{
		DirectFilepath:     "recordings/output.ogg",
		Formats:            []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus},
		ParticipantConfigs: make(map[string]*ParticipantAudioConfig),
	}

	mixed := c.BuildMixedFilepath(types.AudioRecordingFormatOGGOpus)
	require.Equal(t, "recordings/output.ogg", mixed)

	prefix := c.buildStoragePrefix()
	require.Equal(t, "recordings", prefix)

	// participant file should be alongside the mix
	c.RoomName = "room"
	c.SessionID = "sess"
	pc := c.AddParticipant("pid", "alice", "tid")
	participantPath := pc.StorageFilepaths[types.AudioRecordingFormatOGGOpus]
	require.True(t, strings.HasPrefix(participantPath, "recordings/"), "participant path: %q", participantPath)
	require.False(t, strings.Contains(participantPath, "output.ogg"), "participant must not overwrite mix: %q", participantPath)
}

// TestBuildMixedFilepath_DirectFilepath_WAV verifies extension detection for WAV.
func TestBuildMixedFilepath_DirectFilepath_WAV(t *testing.T) {
	c := &AudioRecordingConfig{
		DirectFilepath: "audio/session.wav",
		Formats:        []types.AudioRecordingFormat{types.AudioRecordingFormatWAVPCM},
	}

	mixed := c.BuildMixedFilepath(types.AudioRecordingFormatWAVPCM)
	require.Equal(t, "audio/session.wav", mixed)
	require.Equal(t, "audio", c.buildStoragePrefix())
}

// TestBuildMixedFilepath_DirectFilepath_NoExtension verifies that a DirectFilepath
// without a recognised extension is used directly as the storage prefix.
func TestBuildMixedFilepath_DirectFilepath_NoExtension(t *testing.T) {
	c := &AudioRecordingConfig{
		DirectFilepath: "recordings/my-session",
		Formats:        []types.AudioRecordingFormat{types.AudioRecordingFormatOGGOpus},
	}

	prefix := c.buildStoragePrefix()
	require.Equal(t, "recordings/my-session", prefix)

	mixed := c.BuildMixedFilepath(types.AudioRecordingFormatOGGOpus)
	require.True(t, strings.HasPrefix(mixed, "recordings/my-session/room_mix_"), "mixed path: %q", mixed)
	require.True(t, strings.HasSuffix(mixed, ".ogg"), "mixed path: %q", mixed)
}

// TestCreateAudioRecordingConfigFromFileOutput_DirectFilepath verifies that an explicit
// Filepath on the request propagates into DirectFilepath.
func TestCreateAudioRecordingConfigFromFileOutput_DirectFilepath(t *testing.T) {
	p := &PipelineConfig{
		TmpDir: t.TempDir(),
		Info: &livekit.EgressInfo{
			EgressId: "egress-1",
			RoomId:   "room-id-1",
			RoomName: "test-room",
		},
		AudioConfig: AudioConfig{AudioFrequency: 48000},
		Outputs: map[types.EgressType][]OutputConfig{
			types.EgressTypeFile: {
				&FileConfig{
					outputConfig:     outputConfig{OutputType: types.OutputTypeOGG},
					ExplicitFilepath: "recordings/output.ogg",
					StorageFilepath:  "recordings/output.ogg",
				},
			},
		},
	}

	ar := p.createAudioRecordingConfigFromFileOutput()
	require.NotNil(t, ar)
	require.Equal(t, "recordings/output.ogg", ar.DirectFilepath)
	require.Equal(t, "recordings/output.ogg", ar.BuildMixedFilepath(types.AudioRecordingFormatOGGOpus))
	require.Equal(t, "recordings", ar.buildStoragePrefix())
}

// TestCreateAudioRecordingConfigFromFileOutput_NoExplicitFilepath verifies that when
// the caller does not set Filepath, DirectFilepath is empty and PathPrefix is used.
func TestCreateAudioRecordingConfigFromFileOutput_NoExplicitFilepath(t *testing.T) {
	p := &PipelineConfig{
		BaseConfig: BaseConfig{AudioRecordingPathPrefix: "custom/{room_name}"},
		TmpDir:     t.TempDir(),
		Info: &livekit.EgressInfo{
			EgressId: "egress-1",
			RoomId:   "room-id-1",
			RoomName: "test-room",
		},
		AudioConfig: AudioConfig{AudioFrequency: 48000},
		Outputs: map[types.EgressType][]OutputConfig{
			types.EgressTypeFile: {
				&FileConfig{
					outputConfig:     outputConfig{OutputType: types.OutputTypeOGG},
					ExplicitFilepath: "", // not set
					StorageFilepath:  "test-room-20260223T120000.ogg",
				},
			},
		},
	}

	ar := p.createAudioRecordingConfigFromFileOutput()
	require.NotNil(t, ar)
	require.Empty(t, ar.DirectFilepath)
	require.Equal(t, "custom/{room_name}", ar.PathPrefix)
	require.True(t, strings.HasPrefix(ar.buildStoragePrefix(), "custom/test-room"), ar.buildStoragePrefix())
}
