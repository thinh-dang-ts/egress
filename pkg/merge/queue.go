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
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/protocol/logger"
)

const (
	// Redis key for the merge job queue
	MergeQueueKey = "livekit:egress:merge:queue"

	// Redis key prefix for job status
	MergeJobStatusKeyPrefix = "livekit:egress:merge:status:"

	// Job visibility timeout (how long a job is invisible after being claimed)
	JobVisibilityTimeout = 5 * time.Minute

	// Maximum retries for a job
	MaxJobRetries = 3
)

// MergeJobStatus represents the status of a merge job
type MergeJobStatus string

const (
	MergeJobStatusPending   MergeJobStatus = "pending"
	MergeJobStatusRunning   MergeJobStatus = "running"
	MergeJobStatusCompleted MergeJobStatus = "completed"
	MergeJobStatusFailed    MergeJobStatus = "failed"
)

// MergeJob represents a merge job in the queue
type MergeJob struct {
	ID           string                   `json:"id"`
	ManifestPath string                   `json:"manifest_path"`
	SessionID    string                   `json:"session_id"`
	Encryption   *config.EncryptionConfig `json:"encryption,omitempty"`
	Status       MergeJobStatus           `json:"status"`
	Retries      int                      `json:"retries"`
	CreatedAt    int64                    `json:"created_at"`
	StartedAt    int64                    `json:"started_at,omitempty"`
	CompletedAt  int64                    `json:"completed_at,omitempty"`
	Error        string                   `json:"error,omitempty"`
	WorkerID     string                   `json:"worker_id,omitempty"`
}

// MergeQueue manages the Redis-based merge job queue
type MergeQueue struct {
	client redis.UniversalClient
}

// NewMergeQueue creates a new merge queue
func NewMergeQueue(client redis.UniversalClient) *MergeQueue {
	return &MergeQueue{
		client: client,
	}
}

// Enqueue adds a new merge job to the queue
func (q *MergeQueue) Enqueue(ctx context.Context, manifestPath, sessionID string, encryption *config.EncryptionConfig) (*MergeJob, error) {
	job := &MergeJob{
		ID:           generateJobID(sessionID),
		ManifestPath: manifestPath,
		SessionID:    sessionID,
		Encryption:   cloneEncryptionConfig(encryption),
		Status:       MergeJobStatusPending,
		CreatedAt:    time.Now().UnixNano(),
	}

	data, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	// Add to queue (RPUSH for FIFO)
	if err := q.client.RPush(ctx, MergeQueueKey, data).Err(); err != nil {
		return nil, err
	}

	// Store job status
	statusKey := MergeJobStatusKeyPrefix + job.ID
	if err := q.client.Set(ctx, statusKey, data, 24*time.Hour).Err(); err != nil {
		logger.Warnw("failed to store job status", err, "jobID", job.ID)
	}

	logger.Infow("merge job enqueued",
		"jobID", job.ID,
		"sessionID", sessionID,
		"manifestPath", manifestPath,
	)

	return job, nil
}

// Dequeue retrieves and claims the next job from the queue
func (q *MergeQueue) Dequeue(ctx context.Context, workerID string) (*MergeJob, error) {
	// LPOP to get the next job
	data, err := q.client.LPop(ctx, MergeQueueKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No jobs available
		}
		return nil, err
	}

	var job MergeJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}

	// Claim the job
	job.Status = MergeJobStatusRunning
	job.StartedAt = time.Now().UnixNano()
	job.WorkerID = workerID

	// Update job status
	if err := q.updateJobStatus(ctx, &job); err != nil {
		// Re-queue the job if we can't update status
		q.Requeue(ctx, &job)
		return nil, err
	}

	logger.Infow("merge job claimed",
		"jobID", job.ID,
		"workerID", workerID,
	)

	return &job, nil
}

// Complete marks a job as completed
func (q *MergeQueue) Complete(ctx context.Context, job *MergeJob) error {
	job.Status = MergeJobStatusCompleted
	job.CompletedAt = time.Now().UnixNano()

	if err := q.updateJobStatus(ctx, job); err != nil {
		return err
	}

	logger.Infow("merge job completed",
		"jobID", job.ID,
		"duration", time.Duration(job.CompletedAt-job.StartedAt),
	)

	return nil
}

// Fail marks a job as failed
func (q *MergeQueue) Fail(ctx context.Context, job *MergeJob, err error) error {
	job.Retries++
	job.Error = err.Error()

	if job.Retries < MaxJobRetries {
		// Retry the job
		job.Status = MergeJobStatusPending
		job.StartedAt = 0
		job.WorkerID = ""

		if requeueErr := q.Requeue(ctx, job); requeueErr != nil {
			logger.Errorw("failed to requeue job", requeueErr, "jobID", job.ID)
		}

		logger.Warnw("merge job failed, will retry", err,
			"jobID", job.ID,
			"retries", job.Retries,
		)
	} else {
		job.Status = MergeJobStatusFailed
		job.CompletedAt = time.Now().UnixNano()

		logger.Errorw("merge job failed permanently",
			err,
			"jobID", job.ID,
			"retries", job.Retries,
		)
	}

	return q.updateJobStatus(ctx, job)
}

// Requeue adds a job back to the queue
func (q *MergeQueue) Requeue(ctx context.Context, job *MergeJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return q.client.LPush(ctx, MergeQueueKey, data).Err()
}

// GetJobStatus retrieves the status of a job
func (q *MergeQueue) GetJobStatus(ctx context.Context, jobID string) (*MergeJob, error) {
	statusKey := MergeJobStatusKeyPrefix + jobID
	data, err := q.client.Get(ctx, statusKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var job MergeJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// GetQueueLength returns the number of jobs in the queue
func (q *MergeQueue) GetQueueLength(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, MergeQueueKey).Result()
}

// updateJobStatus updates the job status in Redis
func (q *MergeQueue) updateJobStatus(ctx context.Context, job *MergeJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	statusKey := MergeJobStatusKeyPrefix + job.ID
	return q.client.Set(ctx, statusKey, data, 24*time.Hour).Err()
}

// generateJobID generates a unique job ID
func generateJobID(sessionID string) string {
	return sessionID + "_" + time.Now().Format("20060102T150405.000")
}

// MergeJobEnqueuerImpl implements the MergeJobEnqueuer interface
type MergeJobEnqueuerImpl struct {
	queue *MergeQueue
}

// NewMergeJobEnqueuer creates a new MergeJobEnqueuer
func NewMergeJobEnqueuer(client redis.UniversalClient) *MergeJobEnqueuerImpl {
	return &MergeJobEnqueuerImpl{
		queue: NewMergeQueue(client),
	}
}

// EnqueueMergeJob implements the MergeJobEnqueuer interface
func (e *MergeJobEnqueuerImpl) EnqueueMergeJob(manifestPath string, sessionID string, encryption *config.EncryptionConfig) error {
	_, err := e.queue.Enqueue(context.Background(), manifestPath, sessionID, encryption)
	return err
}

// Wait is a no-op for the Redis-based enqueuer since jobs are processed by separate workers.
func (e *MergeJobEnqueuerImpl) Wait() {}
