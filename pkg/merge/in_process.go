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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/storage"
)

// InProcessMergeEnqueuer implements the MergeJobEnqueuer interface by running
// merge jobs directly in the handler process (in a goroutine), bypassing Redis.
type InProcessMergeEnqueuer struct {
	workerConfig *MergeWorkerConfig
	storage      storage.Storage
}

// NewInProcessMergeEnqueuer creates a new InProcessMergeEnqueuer.
func NewInProcessMergeEnqueuer(storageConfig *config.StorageConfig) (*InProcessMergeEnqueuer, error) {
	tmpDir := config.TmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	cfg := &MergeWorkerConfig{
		WorkerID:      fmt.Sprintf("in-process-%d", time.Now().UnixNano()),
		TmpDir:        tmpDir,
		StorageConfig: storageConfig,
	}

	var store storage.Storage
	var err error
	if storageConfig != nil {
		store, err = getStorage(storageConfig)
		if err != nil {
			return nil, err
		}
	}

	return &InProcessMergeEnqueuer{
		workerConfig: cfg,
		storage:      store,
	}, nil
}

// EnqueueMergeJob runs the merge job directly in a goroutine.
func (e *InProcessMergeEnqueuer) EnqueueMergeJob(manifestPath string, sessionID string) error {
	job := &MergeJob{
		ID:           generateJobID(sessionID),
		ManifestPath: manifestPath,
		SessionID:    sessionID,
		Status:       MergeJobStatusRunning,
		CreatedAt:    time.Now().UnixNano(),
		StartedAt:    time.Now().UnixNano(),
		WorkerID:     e.workerConfig.WorkerID,
	}

	logger.Infow("starting in-process merge job",
		"jobID", job.ID,
		"sessionID", sessionID,
	)

	go func() {
		if err := ProcessMergeJob(context.Background(), e.workerConfig, e.storage, job); err != nil {
			logger.Errorw("in-process merge job failed", err,
				"jobID", job.ID,
				"sessionID", sessionID,
			)
			return
		}
		logger.Infow("in-process merge job completed",
			"jobID", job.ID,
			"sessionID", sessionID,
		)
	}()

	return nil
}
