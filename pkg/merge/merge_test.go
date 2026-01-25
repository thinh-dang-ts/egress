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

import (
	"testing"
	"time"
)

func TestAlignmentInfoOffset(t *testing.T) {
	info := &AlignmentInfo{
		ParticipantID: "test",
		Offset:        2 * time.Second,
		Duration:      60 * time.Second,
	}

	// Test GetOffsetNanoseconds
	expectedNs := int64(2 * time.Second)
	if info.GetOffsetNanoseconds() != expectedNs {
		t.Errorf("expected offset %d ns, got %d ns", expectedNs, info.GetOffsetNanoseconds())
	}

	// Test GetIdentityTsOffset (same as offset in nanoseconds)
	if info.GetIdentityTsOffset() != expectedNs {
		t.Errorf("expected ts-offset %d, got %d", expectedNs, info.GetIdentityTsOffset())
	}
}

func TestEstimateClockDrift(t *testing.T) {
	// 1 hour should have ~10ms drift
	drift := estimateClockDrift(time.Hour)
	expectedDrift := 10 * time.Millisecond

	if drift != expectedDrift {
		t.Errorf("expected drift %v for 1 hour, got %v", expectedDrift, drift)
	}

	// 6 hours should have ~60ms drift
	drift6h := estimateClockDrift(6 * time.Hour)
	expectedDrift6h := 60 * time.Millisecond

	if drift6h != expectedDrift6h {
		t.Errorf("expected drift %v for 6 hours, got %v", expectedDrift6h, drift6h)
	}

	// 30 minutes should have ~5ms drift
	drift30m := estimateClockDrift(30 * time.Minute)
	expectedDrift30m := 5 * time.Millisecond

	if drift30m != expectedDrift30m {
		t.Errorf("expected drift %v for 30 minutes, got %v", expectedDrift30m, drift30m)
	}
}

func TestGenerateJobID(t *testing.T) {
	sessionID := "session-123"
	jobID := generateJobID(sessionID)

	if jobID == "" {
		t.Error("expected non-empty job ID")
	}

	if len(jobID) < len(sessionID) {
		t.Errorf("expected job ID to be longer than session ID, got %s", jobID)
	}

	// Job ID should start with session ID
	if jobID[:len(sessionID)] != sessionID {
		t.Errorf("expected job ID to start with session ID, got %s", jobID)
	}

	t.Logf("Generated job ID: %s", jobID)
}

func TestMergeJobStatus(t *testing.T) {
	// Test job status constants
	statuses := []MergeJobStatus{
		MergeJobStatusPending,
		MergeJobStatusRunning,
		MergeJobStatusCompleted,
		MergeJobStatusFailed,
	}

	for _, status := range statuses {
		if status == "" {
			t.Errorf("status should not be empty")
		}
	}

	// Verify expected values
	if MergeJobStatusPending != "pending" {
		t.Errorf("expected pending status to be 'pending', got %s", MergeJobStatusPending)
	}
	if MergeJobStatusRunning != "running" {
		t.Errorf("expected running status to be 'running', got %s", MergeJobStatusRunning)
	}
	if MergeJobStatusCompleted != "completed" {
		t.Errorf("expected completed status to be 'completed', got %s", MergeJobStatusCompleted)
	}
	if MergeJobStatusFailed != "failed" {
		t.Errorf("expected failed status to be 'failed', got %s", MergeJobStatusFailed)
	}
}

func TestMergeJob(t *testing.T) {
	now := time.Now().UnixNano()
	job := &MergeJob{
		ID:           "test-job-123",
		ManifestPath: "s3://bucket/manifest.json",
		SessionID:    "session-456",
		Status:       MergeJobStatusPending,
		CreatedAt:    now,
	}

	if job.ID == "" {
		t.Error("job ID should not be empty")
	}
	if job.ManifestPath == "" {
		t.Error("manifest path should not be empty")
	}
	if job.SessionID == "" {
		t.Error("session ID should not be empty")
	}
	if job.Status != MergeJobStatusPending {
		t.Errorf("expected status %s, got %s", MergeJobStatusPending, job.Status)
	}
	if job.CreatedAt != now {
		t.Errorf("expected createdAt %d, got %d", now, job.CreatedAt)
	}
}

func TestAlignmentResult(t *testing.T) {
	result := &AlignmentResult{
		ReferenceStart: time.Now().UnixNano(),
		TotalDuration:  10 * time.Minute,
		Alignments: []*AlignmentInfo{
			{
				ParticipantID: "participant-1",
				Offset:        0,
				Duration:      10 * time.Minute,
				ClockDrift:    1 * time.Millisecond,
			},
			{
				ParticipantID: "participant-2",
				Offset:        2 * time.Minute,
				Duration:      8 * time.Minute,
				ClockDrift:    800 * time.Microsecond,
			},
		},
	}

	if len(result.Alignments) != 2 {
		t.Errorf("expected 2 alignments, got %d", len(result.Alignments))
	}

	if result.TotalDuration != 10*time.Minute {
		t.Errorf("expected total duration %v, got %v", 10*time.Minute, result.TotalDuration)
	}

	// First participant should have 0 offset
	if result.Alignments[0].Offset != 0 {
		t.Errorf("expected first participant offset 0, got %v", result.Alignments[0].Offset)
	}

	// Second participant should have 2 minute offset
	if result.Alignments[1].Offset != 2*time.Minute {
		t.Errorf("expected second participant offset 2m, got %v", result.Alignments[1].Offset)
	}
}
