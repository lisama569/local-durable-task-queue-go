package taskqueue

import (
	"context"
	"time"
)

// store is the durability boundary. All state changes go through a store so
// that concurrency control and crash recovery live in one place.
type store interface {
	// PutTask inserts a task. A duplicate idempotency ID returns the existing
	// task (existing == true) instead of inserting.
	PutTask(ctx context.Context, t *Task) (stored *Task, existing bool, err error)
	// GetTask loads one task.
	GetTask(ctx context.Context, id string) (*Task, error)
	// ClaimNext first sweeps expired leases back to pending, then atomically
	// marks the next runnable task running and returns it. ErrNotFound when
	// nothing is runnable.
	ClaimNext(ctx context.Context, now time.Time, workerID string, leaseTTL time.Duration) (*Task, error)
	// SaveTask persists a mutated task via compare-and-swap on FencingToken.
	SaveTask(ctx context.Context, t *Task) error
	// CompleteTask atomically validates the lease (state running, token
	// match) and moves the task to succeeded.
	CompleteTask(ctx context.Context, id string, token int64, now time.Time) (*Task, error)
	// ReportFailure atomically validates the lease and applies mut to the
	// task, then persists it. The mutator must only mutate the passed task.
	ReportFailure(ctx context.Context, id string, token int64, now time.Time, mut func(*Task)) (*Task, error)
	// ListTasks returns all tasks in stable (ID) order.
	ListTasks(ctx context.Context) ([]*Task, error)
	// RecoverExpired moves tasks whose leases expired back to pending.
	RecoverExpired(ctx context.Context, now time.Time) ([]string, error)
	// RecoverOnOpen re-enqueues running leases abandoned by a crashed process.
	RecoverOnOpen(ctx context.Context, now time.Time) ([]string, error)
	// Close releases resources and fsyncs pending writes.
	Close() error
}
