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

package builder

import (
	"fmt"

	"github.com/go-gst/go-gst/gst"
	"github.com/linkdata/deadlock"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/gstreamer"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const (
	audioRecordingBinName = "audio_recording"

	// Minimal buffer for recording (20ms)
	recordingQueueLatency = 20 * 1000 * 1000 // 20ms in nanoseconds
)

// AudioRecordingBin manages per-participant isolated audio recording pipelines
type AudioRecordingBin struct {
	pipeline *gstreamer.Pipeline
	conf     *config.PipelineConfig
	arConf   *config.AudioRecordingConfig

	mu               deadlock.Mutex
	participantBins  map[string]*ParticipantRecordingBin
	nextID           int

	// Callbacks for participant events
	onParticipantAdded   func(participantID string)
	onParticipantRemoved func(participantID string)
}

// ParticipantRecordingBin holds the GStreamer bin for a single participant's audio
type ParticipantRecordingBin struct {
	ParticipantID       string
	ParticipantIdentity string
	TrackID             string
	Bin                 *gstreamer.Bin
	FileSinks           map[types.AudioRecordingFormat]*gst.Element
}

// NewAudioRecordingBin creates a new audio recording bin manager
func NewAudioRecordingBin(pipeline *gstreamer.Pipeline, conf *config.PipelineConfig) (*AudioRecordingBin, error) {
	arConf := conf.GetAudioRecordingConfig()
	if arConf == nil {
		return nil, errors.ErrInvalidInput("audio recording config not found")
	}

	b := &AudioRecordingBin{
		pipeline:        pipeline,
		conf:            conf,
		arConf:          arConf,
		participantBins: make(map[string]*ParticipantRecordingBin),
	}

	// Set up track callbacks
	pipeline.AddOnTrackAdded(b.onTrackAdded)
	pipeline.AddOnTrackRemoved(b.onTrackRemoved)

	return b, nil
}

// SetOnParticipantAdded sets the callback for when a participant is added
func (b *AudioRecordingBin) SetOnParticipantAdded(cb func(participantID string)) {
	b.onParticipantAdded = cb
}

// SetOnParticipantRemoved sets the callback for when a participant is removed
func (b *AudioRecordingBin) SetOnParticipantRemoved(cb func(participantID string)) {
	b.onParticipantRemoved = cb
}

// onTrackAdded handles new audio tracks joining the session
func (b *AudioRecordingBin) onTrackAdded(ts *config.TrackSource) {
	if ts.TrackKind != lksdk.TrackKindAudio {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	logger.Debugw("adding audio recording bin for track",
		"trackID", ts.TrackID,
		"mimeType", ts.MimeType,
	)

	if err := b.addParticipantBin(ts); err != nil {
		logger.Errorw("failed to add participant recording bin", err, "trackID", ts.TrackID)
		return
	}

	if b.onParticipantAdded != nil {
		// Extract participant ID from track source (will be set by SDK source)
		// For now, use trackID as participant identifier
		b.onParticipantAdded(ts.TrackID)
	}
}

// onTrackRemoved handles audio tracks leaving the session
func (b *AudioRecordingBin) onTrackRemoved(trackID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pBin, ok := b.participantBins[trackID]
	if !ok {
		return
	}

	logger.Debugw("removing audio recording bin for track", "trackID", trackID)

	// The bin will flush and finalize on removal
	if err := pBin.Bin.SetState(gstreamer.StateNull); err != nil {
		logger.Errorw("failed to set participant bin state to null", err, "trackID", trackID)
	}

	delete(b.participantBins, trackID)

	if b.onParticipantRemoved != nil {
		b.onParticipantRemoved(trackID)
	}
}

// addParticipantBin creates an isolated recording pipeline for a participant
func (b *AudioRecordingBin) addParticipantBin(ts *config.TrackSource) error {
	binName := fmt.Sprintf("%s_%s_%d", audioRecordingBinName, ts.TrackID, b.nextID)
	b.nextID++

	bin := b.pipeline.NewBin(binName)

	// Configure app source
	ts.AppSrc.Element.SetArg("format", "time")
	if err := ts.AppSrc.Element.SetProperty("is-live", true); err != nil {
		return err
	}

	if err := bin.AddElement(ts.AppSrc.Element); err != nil {
		return err
	}

	// Add depayloader and decoder based on codec
	if err := b.addDepayloaderAndDecoder(bin, ts); err != nil {
		return err
	}

	// Add audio conversion chain (without mixer - isolated per participant)
	if err := b.addAudioConversion(bin); err != nil {
		return err
	}

	// Create file sinks for each format
	fileSinks := make(map[types.AudioRecordingFormat]*gst.Element)

	if len(b.arConf.Formats) == 1 {
		// Single format - direct connection
		sink, err := b.createFileSink(bin, ts.TrackID, b.arConf.Formats[0])
		if err != nil {
			return err
		}
		fileSinks[b.arConf.Formats[0]] = sink
	} else {
		// Multiple formats - use tee
		tee, err := gst.NewElementWithName("tee", fmt.Sprintf("%s_tee", binName))
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = bin.AddElement(tee); err != nil {
			return err
		}

		for _, format := range b.arConf.Formats {
			// Create queue for this branch
			queue, err := gstreamer.BuildQueue(
				fmt.Sprintf("%s_%s_queue", binName, format),
				recordingQueueLatency,
				blockingQueue,
			)
			if err != nil {
				return err
			}
			if err = bin.AddElement(queue); err != nil {
				return err
			}

			sink, err := b.createFileSink(bin, ts.TrackID, format)
			if err != nil {
				return err
			}
			fileSinks[format] = sink
		}
	}

	pBin := &ParticipantRecordingBin{
		ParticipantID: ts.TrackID, // Will be updated with actual participant ID
		TrackID:       ts.TrackID,
		Bin:           bin,
		FileSinks:     fileSinks,
	}

	b.participantBins[ts.TrackID] = pBin

	return b.pipeline.AddSourceBin(bin)
}

// addDepayloaderAndDecoder adds RTP depayloader and audio decoder
func (b *AudioRecordingBin) addDepayloaderAndDecoder(bin *gstreamer.Bin, ts *config.TrackSource) error {
	switch ts.MimeType {
	case types.MimeTypeOpus:
		if err := ts.AppSrc.Element.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=audio,payload=%d,encoding-name=OPUS,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		rtpOpusDepay, err := gst.NewElement("rtpopusdepay")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		opusDec, err := gst.NewElement("opusdec")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		return bin.AddElements(rtpOpusDepay, opusDec)

	case types.MimeTypePCMU:
		if err := ts.AppSrc.Element.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=audio,payload=%d,encoding-name=PCMU,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		rtpPCMUDepay, err := gst.NewElement("rtppcmudepay")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		mulawDec, err := gst.NewElement("mulawdec")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		return bin.AddElements(rtpPCMUDepay, mulawDec)

	case types.MimeTypePCMA:
		if err := ts.AppSrc.Element.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=audio,payload=%d,encoding-name=PCMA,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		rtpPCMADepay, err := gst.NewElement("rtppcmadepay")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		alawDec, err := gst.NewElement("alawdec")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		return bin.AddElements(rtpPCMADepay, alawDec)

	default:
		return errors.ErrNotSupported(string(ts.MimeType))
	}
}

// addAudioConversion adds the audio conversion chain
func (b *AudioRecordingBin) addAudioConversion(bin *gstreamer.Bin) error {
	// Minimal queue for recording (20ms instead of 500ms jitter buffer)
	queue, err := gstreamer.BuildQueue(
		fmt.Sprintf("%s_conv_queue", bin.GetName()),
		recordingQueueLatency,
		blockingQueue,
	)
	if err != nil {
		return err
	}

	audioConvert, err := gst.NewElement("audioconvert")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	audioResample, err := gst.NewElement("audioresample")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	// Create caps filter for target sample rate and stereo output
	capsFilter, err := b.createRecordingCapsFilter()
	if err != nil {
		return err
	}

	return bin.AddElements(queue, audioConvert, audioResample, capsFilter)
}

// createRecordingCapsFilter creates caps filter for the recording output format
func (b *AudioRecordingBin) createRecordingCapsFilter() (*gst.Element, error) {
	// Stereo, 16-bit PCM at configured sample rate
	caps := gst.NewCapsFromString(fmt.Sprintf(
		"audio/x-raw,format=S16LE,layout=interleaved,rate=%d,channels=2",
		b.arConf.SampleRate,
	))

	capsFilter, err := gst.NewElement("capsfilter")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = capsFilter.SetProperty("caps", caps); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	return capsFilter, nil
}

// createFileSink creates the encoder, muxer, and file sink for a format
func (b *AudioRecordingBin) createFileSink(bin *gstreamer.Bin, trackID string, format types.AudioRecordingFormat) (*gst.Element, error) {
	pConfig := b.arConf.GetParticipantConfig(trackID)
	if pConfig == nil {
		return nil, errors.ErrInvalidInput("participant config not found")
	}

	localFilepath := pConfig.LocalFilepaths[format]

	switch format {
	case types.AudioRecordingFormatOGGOpus:
		return b.createOggOpusSink(bin, localFilepath)
	case types.AudioRecordingFormatWAVPCM:
		return b.createWavPcmSink(bin, localFilepath)
	default:
		return nil, errors.ErrNotSupported(string(format))
	}
}

// createOggOpusSink creates encoder, muxer, and sink for OGG/Opus format
func (b *AudioRecordingBin) createOggOpusSink(bin *gstreamer.Bin, filepath string) (*gst.Element, error) {
	// Opus encoder
	opusEnc, err := gst.NewElement("opusenc")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	// Configure for speech-tuned, VBR
	// Bitrate ceiling based on sample rate
	bitrate := b.getBitrateForSampleRate()
	if err = opusEnc.SetProperty("bitrate", int(bitrate*1000)); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = opusEnc.SetProperty("audio-type", 2001); err != nil { // 2001 = voice
		// Property may not exist in all versions, ignore error
		logger.Debugw("could not set opus audio-type", "error", err)
	}

	// OGG muxer
	oggMux, err := gst.NewElement("oggmux")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	// File sink
	fileSink, err := gst.NewElement("filesink")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = fileSink.SetProperty("location", filepath); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = fileSink.SetProperty("sync", false); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	return fileSink, bin.AddElements(opusEnc, oggMux, fileSink)
}

// createWavPcmSink creates muxer and sink for WAV/PCM format
func (b *AudioRecordingBin) createWavPcmSink(bin *gstreamer.Bin, filepath string) (*gst.Element, error) {
	// WAV encoder (16-bit LE stereo PCM, no actual encoding needed)
	wavEnc, err := gst.NewElement("wavenc")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	// File sink
	fileSink, err := gst.NewElement("filesink")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = fileSink.SetProperty("location", filepath); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if err = fileSink.SetProperty("sync", false); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}

	return fileSink, bin.AddElements(wavEnc, fileSink)
}

// getBitrateForSampleRate returns appropriate bitrate for the sample rate
func (b *AudioRecordingBin) getBitrateForSampleRate() int32 {
	switch b.arConf.SampleRate {
	case 8000:
		return 24 // 24 kbps
	case 16000:
		return 32 // 32 kbps
	case 24000:
		return 48 // 48 kbps
	case 32000:
		return 64 // 64 kbps
	case 44100, 48000:
		return 96 // 96 kbps
	default:
		return 64 // Default to 64 kbps
	}
}

// GetParticipantBin returns the bin for a specific participant
func (b *AudioRecordingBin) GetParticipantBin(trackID string) *ParticipantRecordingBin {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.participantBins[trackID]
}

// GetAllParticipantBins returns all participant bins
func (b *AudioRecordingBin) GetAllParticipantBins() map[string]*ParticipantRecordingBin {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make(map[string]*ParticipantRecordingBin)
	for k, v := range b.participantBins {
		result[k] = v
	}
	return result
}

// Close stops all participant bins
func (b *AudioRecordingBin) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for trackID, pBin := range b.participantBins {
		if err := pBin.Bin.SetState(gstreamer.StateNull); err != nil {
			logger.Errorw("failed to close participant bin", err, "trackID", trackID)
		}
	}

	return nil
}
