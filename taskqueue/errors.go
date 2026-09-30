package taskqueue

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the queue.
var (
	// ErrNotFound is returned when no task matches the request.
	ErrNotFound = errors.New("taskqueue: task not found")
	// ErrTerminal is returned when an operation targets a task already in a
	// terminal state (succeeded or dead).
	ErrTerminal = errors.New("taskqueue: task is in a terminal state")
	// ErrFencing is returned when a completion/failure report carries a stale
	// fencing token (e.g. a report for an expired lease).
	ErrFencing = errors.New("taskqueue: stale fencing token (lease lost)")
	// ErrNotRunning is returned when a report targets a task that is not
	// currently leased by this token.
	ErrNotRunning = errors.New("taskqueue: task is not running")
	// ErrQueueClosed is returned after the queue has been closed.
	ErrQueueClosed = errors.New("taskqueue: queue is closed")
	// ErrInvalidArgument is returned for malformed requests.
	ErrInvalidArgument = errors.New("taskqueue: invalid argument")
)

// TaskError attaches a task ID and operation to a cause.
type TaskError struct {
	TaskID string
	Op     string
	Err    error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("taskqueue: %s task %s: %v", e.Op, e.TaskID, e.Err)
}

func (e *TaskError) Unwrap() error { return e.Err }
