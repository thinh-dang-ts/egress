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
	"testing"
)

func TestNewInProcessMergeEnqueuer(t *testing.T) {
	// Test creation with nil storage config (uses local storage)
	enqueuer, err := NewInProcessMergeEnqueuer(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if enqueuer == nil {
		t.Fatal("expected non-nil enqueuer")
	}
	if enqueuer.workerConfig == nil {
		t.Fatal("expected non-nil worker config")
	}
	if enqueuer.workerConfig.WorkerID == "" {
		t.Error("expected non-empty worker ID")
	}
	if enqueuer.workerConfig.TmpDir == "" {
		t.Error("expected non-empty tmp dir")
	}
}

func TestInProcessMergeEnqueuerEnqueueReturnsNil(t *testing.T) {
	enqueuer, err := NewInProcessMergeEnqueuer(nil)
	if err != nil {
		t.Fatalf("expected no error creating enqueuer, got %v", err)
	}

	// EnqueueMergeJob should return nil immediately (goroutine runs async)
	// The goroutine will fail because there's no real manifest, but that's expected
	err = enqueuer.EnqueueMergeJob("/nonexistent/manifest.json", "test-session", nil)
	if err != nil {
		t.Fatalf("expected EnqueueMergeJob to return nil, got %v", err)
	}
}
