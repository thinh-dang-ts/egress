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

//go:build cgo

package merge

import (
	"sort"
	"time"

	"github.com/livekit/egress/pkg/config"
)

// ComputeAlignment computes alignment information for merging multiple audio tracks
// The algorithm uses server timestamps as the primary alignment reference,
// with RTP clock info for fine-tuning within each track.
//
// Target: <20ms drift over 60 minutes
func ComputeAlignment(manifest *config.AudioRecordingManifest) *AlignmentResult {
	if len(manifest.Participants) == 0 {
		return &AlignmentResult{}
	}

	result := &AlignmentResult{
		Alignments: make([]*AlignmentInfo, 0, len(manifest.Participants)),
	}

	// Find the earliest server timestamp as reference
	var earliestServerTime int64 = -1
	for _, p := range manifest.Participants {
		if p.ClockSync != nil && p.ClockSync.ServerTimestamp > 0 {
			if earliestServerTime < 0 || p.ClockSync.ServerTimestamp < earliestServerTime {
				earliestServerTime = p.ClockSync.ServerTimestamp
			}
		} else if p.JoinedAt > 0 {
			// Fall back to join time if no clock sync info
			if earliestServerTime < 0 || p.JoinedAt < earliestServerTime {
				earliestServerTime = p.JoinedAt
			}
		}
	}

	if earliestServerTime < 0 {
		earliestServerTime = manifest.StartedAt
	}
	result.ReferenceStart = earliestServerTime

	// Compute alignment for each participant
	maxEndTime := earliestServerTime
	for _, p := range manifest.Participants {
		info := computeParticipantAlignment(p, earliestServerTime)
		result.Alignments = append(result.Alignments, info)

		// Track max end time for total duration
		endTime := earliestServerTime + int64(info.Offset+info.Duration)
		if endTime > maxEndTime {
			maxEndTime = endTime
		}
	}

	result.TotalDuration = time.Duration(maxEndTime - earliestServerTime)

	// Sort alignments by offset for easier processing
	sort.Slice(result.Alignments, func(i, j int) bool {
		return result.Alignments[i].Offset < result.Alignments[j].Offset
	})

	return result
}

// computeParticipantAlignment computes alignment for a single participant
func computeParticipantAlignment(p *config.ParticipantRecordingInfo, referenceStart int64) *AlignmentInfo {
	info := &AlignmentInfo{
		ParticipantID: p.ParticipantID,
	}

	// Compute offset from reference start
	var startTime int64
	if p.ClockSync != nil && p.ClockSync.ServerTimestamp > 0 {
		startTime = p.ClockSync.ServerTimestamp
	} else {
		startTime = p.JoinedAt
	}

	if startTime > referenceStart {
		info.Offset = time.Duration(startTime - referenceStart)
	}

	// Compute duration from artifacts or join/leave times
	info.Duration = computeParticipantDuration(p)

	// Estimate clock drift
	// For high-quality audio recording, we target <20ms drift over 60 minutes
	// This is typically achieved by using server timestamps as primary reference
	// RTP clock drift is usually <10ms per hour with modern implementations
	info.ClockDrift = estimateClockDrift(info.Duration)

	return info
}

// computeParticipantDuration computes the duration of a participant's recording
func computeParticipantDuration(p *config.ParticipantRecordingInfo) time.Duration {
	// Try to get duration from artifacts
	for _, artifact := range p.Artifacts {
		if artifact.DurationMs > 0 {
			return time.Duration(artifact.DurationMs) * time.Millisecond
		}
	}

	// Fall back to join/leave times
	if p.JoinedAt > 0 && p.LeftAt > 0 {
		return time.Duration(p.LeftAt - p.JoinedAt)
	}

	// Default to 0 if we can't determine duration
	return 0
}
