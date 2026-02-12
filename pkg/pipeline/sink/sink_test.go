package sink

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/egress/pkg/config"
)

func TestWireMergeEnqueuer_PrefersAudioRecordingStorageConfig(t *testing.T) {
	originalFactory := newInProcessMergeEnqueuer
	t.Cleanup(func() {
		newInProcessMergeEnqueuer = originalFactory
	})

	var got *config.StorageConfig
	enqueuer := &mockMergeEnqueuer{}
	newInProcessMergeEnqueuer = func(storageConfig *config.StorageConfig) (MergeJobEnqueuer, error) {
		got = storageConfig
		return enqueuer, nil
	}

	globalStorage := &config.StorageConfig{Prefix: "global-prefix"}
	requestStorage := &config.StorageConfig{Prefix: "request-prefix"}

	s := &AudioRecordingSink{
		arConf: &config.AudioRecordingConfig{
			StorageConfig: requestStorage,
		},
	}
	conf := &config.PipelineConfig{
		BaseConfig: config.BaseConfig{
			MergeInProcess: true,
			StorageConfig:  globalStorage,
		},
	}

	wireMergeEnqueuer(s, conf)

	require.Same(t, requestStorage, got)
	require.Same(t, enqueuer, s.mergeJobEnqueuer)
}

func TestWireMergeEnqueuer_FallsBackToPipelineStorageConfig(t *testing.T) {
	originalFactory := newInProcessMergeEnqueuer
	t.Cleanup(func() {
		newInProcessMergeEnqueuer = originalFactory
	})

	var got *config.StorageConfig
	enqueuer := &mockMergeEnqueuer{}
	newInProcessMergeEnqueuer = func(storageConfig *config.StorageConfig) (MergeJobEnqueuer, error) {
		got = storageConfig
		return enqueuer, nil
	}

	globalStorage := &config.StorageConfig{Prefix: "global-prefix"}

	s := &AudioRecordingSink{
		arConf: &config.AudioRecordingConfig{},
	}
	conf := &config.PipelineConfig{
		BaseConfig: config.BaseConfig{
			MergeInProcess: true,
			StorageConfig:  globalStorage,
		},
	}

	wireMergeEnqueuer(s, conf)

	require.Same(t, globalStorage, got)
	require.Same(t, enqueuer, s.mergeJobEnqueuer)
}
