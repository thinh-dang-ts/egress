package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrackSourceOnClockSyncInfoReceivesFutureCapture(t *testing.T) {
	ts := &TrackSource{}

	got := make(chan *ClockSyncInfo, 1)
	ts.OnClockSyncInfo(func(info *ClockSyncInfo) {
		got <- info
	})

	expected := &ClockSyncInfo{
		ServerTimestamp: 12345,
		RTPClockBase:    67890,
		ClockRate:       48000,
	}
	ts.SetClockSyncInfo(expected)

	select {
	case info := <-got:
		require.Equal(t, expected, info)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for clock sync callback")
	}
}

func TestTrackSourceOnClockSyncInfoReceivesCachedCaptureImmediately(t *testing.T) {
	ts := &TrackSource{}
	expected := &ClockSyncInfo{
		ServerTimestamp: 111,
		RTPClockBase:    222,
		ClockRate:       48000,
	}
	ts.SetClockSyncInfo(expected)

	called := false
	ts.OnClockSyncInfo(func(info *ClockSyncInfo) {
		called = true
		require.Equal(t, expected, info)
	})

	require.True(t, called, "expected immediate callback for cached clock sync info")
}

func TestTrackSourceSetClockSyncInfoFirstWins(t *testing.T) {
	ts := &TrackSource{}
	first := &ClockSyncInfo{
		ServerTimestamp: 1,
		RTPClockBase:    2,
		ClockRate:       48000,
	}
	second := &ClockSyncInfo{
		ServerTimestamp: 3,
		RTPClockBase:    4,
		ClockRate:       48000,
	}

	ts.SetClockSyncInfo(first)
	ts.SetClockSyncInfo(second)

	require.Equal(t, first, ts.GetClockSyncInfo())
}
