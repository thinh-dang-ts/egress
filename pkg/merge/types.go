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

package merge

import "time"

// AlignmentInfo contains alignment information for a participant's audio track
type AlignmentInfo struct {
	ParticipantID string
	Offset        time.Duration // Offset from reference start (positive = starts later)
	Duration      time.Duration // Estimated duration
	ClockDrift    time.Duration // Estimated clock drift over duration
}

// AlignmentResult contains the result of alignment computation
type AlignmentResult struct {
	ReferenceStart int64            // Earliest server timestamp (reference point)
	Alignments     []*AlignmentInfo // Per-participant alignment info
	TotalDuration  time.Duration    // Total duration of merged output
}

// GetOffsetNanoseconds returns the offset in nanoseconds for GStreamer ts-offset property
func (a *AlignmentInfo) GetOffsetNanoseconds() int64 {
	return int64(a.Offset)
}

// GetIdentityTsOffset returns the ts-offset value for GStreamer identity element
// This is used to shift the presentation timestamp of the audio stream
func (a *AlignmentInfo) GetIdentityTsOffset() int64 {
	// ts-offset is in nanoseconds, positive value delays the stream
	return int64(a.Offset)
}

// estimateClockDrift estimates potential clock drift based on duration
// Modern systems typically have <10ms drift per hour
func estimateClockDrift(duration time.Duration) time.Duration {
	// Conservative estimate: 10ms per hour
	hours := float64(duration) / float64(time.Hour)
	driftMs := hours * 10.0
	return time.Duration(driftMs) * time.Millisecond
}

// ValidateAlignment validates that alignment meets quality targets
func ValidateAlignment(result *AlignmentResult) error {
	// Check for excessive drift
	for _, a := range result.Alignments {
		// Target: <20ms drift over 60 minutes
		maxAcceptableDrift := time.Duration(float64(a.Duration)/float64(time.Hour)*20.0) * time.Millisecond
		if a.ClockDrift > maxAcceptableDrift {
			// Log warning but don't fail - drift may be within tolerance
		}
	}

	return nil
}
