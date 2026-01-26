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

package sdk

import (
	"io"
	"net"
	"time"

	"github.com/frostbyte73/core"
	"github.com/go-gst/go-gst/gst"
	"github.com/go-gst/go-gst/gst/app"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/gstreamer"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const (
	// Minimal ring buffer for recording (20ms instead of 500ms jitter buffer)
	recordingRingBufferSize = 20 * time.Millisecond

	// Read timeout
	recordingReadTimeout = 500 * time.Millisecond

	// Maximum packets to buffer before dropping
	maxRecordingRingSize = 50
)

// RecordingAppWriter is a simplified AppWriter optimized for audio recording
// Key optimizations:
// - 20ms ring buffer instead of 500ms jitter buffer (95% memory reduction)
// - No complex synchronizer - captures RTP clock info for offline alignment
// - Direct push to GStreamer without sample queue depth management
// - Clock sync info capture for offline merge alignment
type RecordingAppWriter struct {
	conf *config.PipelineConfig

	logger logger.Logger

	pub         lksdk.TrackPublication
	track       *webrtc.TrackRemote
	codec       types.MimeType
	src         *app.Source
	startTime   time.Time
	trackSource *config.TrackSource

	// Minimal ring buffer
	ringBuffer   []*rtp.Packet
	ringHead     int
	ringTail     int
	ringSize     int
	ringCapacity int

	// Clock sync info for offline alignment
	clockSyncInfo    *config.ClockSyncInfo
	firstPacketTime  int64
	firstRTPTimestamp uint32
	clockRate        uint32
	clockInfoCaptured bool

	// Depacketizer
	depacketizer rtp.Depacketizer
	translator   Translator

	// Pipeline callbacks
	callbacks *gstreamer.Callbacks

	// State
	active        atomic.Bool
	lastReceived  atomic.Time
	lastPTS       time.Duration
	initialized   bool
	playing       core.Fuse
	draining      core.Fuse
	finished      core.Fuse

	// Stats
	packetsReceived atomic.Uint64
	packetsPushed   atomic.Uint64
	packetsDropped  atomic.Uint64

	// Callback for clock sync capture
	onClockSyncCaptured func(*config.ClockSyncInfo)
}

// NewRecordingAppWriter creates a new recording-optimized AppWriter
func NewRecordingAppWriter(
	conf *config.PipelineConfig,
	track *webrtc.TrackRemote,
	pub lksdk.TrackPublication,
	ts *config.TrackSource,
	callbacks *gstreamer.Callbacks,
) (*RecordingAppWriter, error) {
	w := &RecordingAppWriter{
		conf:         conf,
		logger:       logger.GetLogger().WithValues("trackID", track.ID(), "kind", track.Kind().String(), "mode", "recording"),
		track:        track,
		pub:          pub,
		codec:        ts.MimeType,
		src:          ts.AppSrc,
		trackSource:  ts,
		callbacks:    callbacks,
		ringCapacity: maxRecordingRingSize,
		ringBuffer:   make([]*rtp.Packet, maxRecordingRingSize),
		clockRate:    ts.ClockRate,
	}

	// Set up depacketizer based on codec
	switch ts.MimeType {
	case types.MimeTypeOpus:
		w.depacketizer = &codecs.OpusPacket{}
		w.translator = NewNullTranslator()

	case types.MimeTypePCMU:
		w.depacketizer = &G711Packet{}
		w.translator = NewNullTranslator()

	case types.MimeTypePCMA:
		w.depacketizer = &G711Packet{}
		w.translator = NewNullTranslator()

	default:
		return nil, errors.ErrNotSupported(string(ts.MimeType))
	}

	go w.start()
	return w, nil
}

// SetOnClockSyncCaptured sets the callback for when clock sync info is captured
func (w *RecordingAppWriter) SetOnClockSyncCaptured(cb func(*config.ClockSyncInfo)) {
	w.onClockSyncCaptured = cb
}

// GetClockSyncInfo returns the captured clock sync info
func (w *RecordingAppWriter) GetClockSyncInfo() *config.ClockSyncInfo {
	return w.clockSyncInfo
}

// start runs the main read and push loop
func (w *RecordingAppWriter) start() {
	w.startTime = time.Now()
	w.active.Store(true)

	go func() {
		<-w.callbacks.BuildReady
		if !w.active.Load() {
			w.callbacks.OnTrackMuted(w.track.ID())
		}
	}()

	go w.pushLoop()

	// Main read loop
	for !w.draining.IsBroken() {
		w.readNext()
	}

	// Flush remaining packets
	w.flushRingBuffer()

	<-w.finished.Watch()
	w.logger.Infow("recording writer finished",
		"packetsReceived", w.packetsReceived.Load(),
		"packetsPushed", w.packetsPushed.Load(),
		"packetsDropped", w.packetsDropped.Load(),
	)
}

// readNext reads the next RTP packet
func (w *RecordingAppWriter) readNext() {
	_ = w.track.SetReadDeadline(time.Now().Add(recordingReadTimeout))
	pkt, _, err := w.track.ReadRTP()
	if err != nil {
		w.handleReadError(err)
		return
	}

	w.packetsReceived.Inc()
	w.lastReceived.Store(time.Now())

	// Capture clock sync info from first packet
	if !w.clockInfoCaptured {
		w.captureClockSyncInfo(pkt)
	}

	// Add to ring buffer
	w.addToRingBuffer(pkt)

	if !w.active.Swap(true) {
		w.logger.Debugw("track active")
		w.callbacks.OnTrackUnmuted(w.track.ID())
	}
}

// captureClockSyncInfo captures timing info from the first packet for offline alignment
func (w *RecordingAppWriter) captureClockSyncInfo(pkt *rtp.Packet) {
	w.firstPacketTime = time.Now().UnixNano()
	w.firstRTPTimestamp = pkt.Timestamp
	w.clockInfoCaptured = true

	w.clockSyncInfo = &config.ClockSyncInfo{
		ServerTimestamp: w.firstPacketTime,
		RTPClockBase:    w.firstRTPTimestamp,
		ClockRate:       w.clockRate,
	}

	w.logger.Debugw("captured clock sync info",
		"serverTimestamp", w.firstPacketTime,
		"rtpClockBase", w.firstRTPTimestamp,
		"clockRate", w.clockRate,
	)

	if w.onClockSyncCaptured != nil {
		w.onClockSyncCaptured(w.clockSyncInfo)
	}
}

// addToRingBuffer adds a packet to the minimal ring buffer
func (w *RecordingAppWriter) addToRingBuffer(pkt *rtp.Packet) {
	// If buffer is full, drop oldest packet
	if w.ringSize >= w.ringCapacity {
		w.ringHead = (w.ringHead + 1) % w.ringCapacity
		w.ringSize--
		w.packetsDropped.Inc()
		w.logger.Warnw("ring buffer full, dropping oldest packet", nil)
	}

	w.ringBuffer[w.ringTail] = pkt
	w.ringTail = (w.ringTail + 1) % w.ringCapacity
	w.ringSize++
}

// popFromRingBuffer removes and returns the oldest packet from the ring buffer
func (w *RecordingAppWriter) popFromRingBuffer() *rtp.Packet {
	if w.ringSize == 0 {
		return nil
	}

	pkt := w.ringBuffer[w.ringHead]
	w.ringBuffer[w.ringHead] = nil
	w.ringHead = (w.ringHead + 1) % w.ringCapacity
	w.ringSize--

	return pkt
}

// pushLoop pushes packets from ring buffer to GStreamer
func (w *RecordingAppWriter) pushLoop() {
	defer w.finished.Break()

	// Wait for pipeline to be ready
	select {
	case <-w.callbacks.BuildReady:
	case <-w.draining.Watch():
		return
	}

	// Wait for playing state
	select {
	case <-w.playing.Watch():
	case <-w.draining.Watch():
		return
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.pushPendingPackets()

		case <-w.draining.Watch():
			// Push any remaining packets
			w.pushPendingPackets()
			return
		}
	}
}

// pushPendingPackets pushes all pending packets from ring buffer
func (w *RecordingAppWriter) pushPendingPackets() {
	for {
		pkt := w.popFromRingBuffer()
		if pkt == nil {
			break
		}

		if err := w.pushPacket(pkt); err != nil {
			w.logger.Warnw("failed to push packet", err)
			w.packetsDropped.Inc()
		}
	}
}

// pushPacket pushes a single packet to GStreamer
func (w *RecordingAppWriter) pushPacket(pkt *rtp.Packet) error {
	w.translator.Translate(pkt)

	// Calculate PTS based on RTP timestamp
	pts := w.calculatePTS(pkt.Timestamp)

	if pts < 0 {
		w.logger.Debugw("negative packet pts, dropping", "pts", pts)
		return nil
	}

	p, err := pkt.Marshal()
	if err != nil {
		return err
	}

	b := gst.NewBufferFromBytes(p)
	b.SetPresentationTimestamp(gst.ClockTime(uint64(pts)))

	if flow := w.src.PushBuffer(b); flow != gst.FlowOK {
		if flow != gst.FlowFlushing {
			w.logger.Warnw("unexpected flow return", nil, "flow", flow.String())
		}
		return nil
	}

	w.packetsPushed.Inc()
	w.lastPTS = pts
	return nil
}

// calculatePTS calculates presentation timestamp from RTP timestamp
func (w *RecordingAppWriter) calculatePTS(rtpTimestamp uint32) time.Duration {
	if !w.clockInfoCaptured || w.clockRate == 0 {
		return 0
	}

	// Calculate delta from first RTP timestamp
	rtpDelta := rtpTimestamp - w.firstRTPTimestamp

	// Convert to nanoseconds: delta * (1e9 / clockRate)
	pts := time.Duration(float64(rtpDelta) * 1e9 / float64(w.clockRate))

	return pts
}

// handleReadError handles errors from track.ReadRTP
func (w *RecordingAppWriter) handleReadError(err error) {
	var netErr net.Error
	switch {
	case w.draining.IsBroken():
		return

	case errors.As(err, &netErr) && netErr.Timeout():
		if !w.active.Load() {
			return
		}
		lastRecv := w.lastReceived.Load()
		if lastRecv.IsZero() {
			lastRecv = w.startTime
		}
		if w.pub.IsMuted() || time.Since(lastRecv) > recordingRingBufferSize*10 {
			w.logger.Debugw("track inactive")
			w.active.Store(false)
			w.callbacks.OnTrackMuted(w.track.ID())
		}

	case errors.Is(err, io.EOF):
		w.logger.Debugw("read EOF, ending recording")
		w.draining.Break()

	default:
		w.logger.Errorw("read error", err)
		w.draining.Break()
	}
}

// flushRingBuffer flushes remaining packets in the ring buffer
func (w *RecordingAppWriter) flushRingBuffer() {
	w.logger.Debugw("flushing ring buffer", "remaining", w.ringSize)
	w.pushPendingPackets()
}

// Playing signals that the pipeline is playing
func (w *RecordingAppWriter) Playing() {
	w.playing.Break()
}

// Drain initiates graceful shutdown
func (w *RecordingAppWriter) Drain(_ bool) {
	w.draining.Once(func() {
		w.logger.Debugw("draining recording writer")
	})

	<-w.finished.Watch()
	w.logger.Debugw("recording writer drained")
}

// IsActive returns whether the track is currently active
func (w *RecordingAppWriter) IsActive() bool {
	return w.active.Load()
}

// TrackKind returns the track kind (always audio for recording)
func (w *RecordingAppWriter) TrackKind() webrtc.RTPCodecType {
	return w.track.Kind()
}

// GetStats returns recording stats
func (w *RecordingAppWriter) GetStats() (packetsReceived, packetsPushed, packetsDropped uint64) {
	return w.packetsReceived.Load(), w.packetsPushed.Load(), w.packetsDropped.Load()
}
