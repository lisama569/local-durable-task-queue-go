// Package taskqueue implements an embeddable, durable local task queue.
//
// It requires no external middleware, services or credentials: state is
// persisted in a local directory using only the Go standard library.
package taskqueue

import "time"

// State is the lifecycle state of a task.
type State int

const (
	StateUnknown   State = iota
	StatePending         // waiting to be claimed (includes tasks waiting for a delayed retry)
	StateRunning         // claimed by a consumer and covered by a lease
	StateSucceeded       // terminal: completed and confirmed exactly once
	StateDead            // terminal: dead-lettered after exhausting retries or a permanent failure
)

func (s State) String() string { return stateName[s] }

var stateName = map[State]string{
	StateUnknown:   "unknown",
	StatePending:   "pending",
	StateRunning:   "running",
	StateSucceeded: "succeeded",
	StateDead:      "dead",
}

// FailureKind classifies a task failure.
type FailureKind int

const (
	FailureKindNone      FailureKind = iota
	FailureKindRetryable             // transient; the task may be retried
	FailureKindPermanent             // non-retryable; the task is dead-lettered immediately
)

func (k FailureKind) String() string { return failureKindName[k] }

var failureKindName = map[FailureKind]string{
	FailureKindNone:      "none",
	FailureKindRetryable: "retryable",
	FailureKindPermanent: "permanent",
}

// Task is the persisted record of one task.
type Task struct {
	ID            string
	IdempotencyID string
	Payload       []byte
	State         State
	Attempt       int
	MaxAttempts   int
	RunAt         time.Time
	LeasedBy      string
	LeaseExpires  time.Time
	FencingToken  int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Failures      []FailureRecord
}

// FailureRecord explains one failed attempt. Records form a reason chain:
// later entries may wrap earlier causes via Cause.
type FailureRecord struct {
	Attempt int
	Kind    FailureKind
	Message string
	Cause   string
	At      time.Time
	NextRun time.Time
	Backoff time.Duration
	Dead    bool
}

// EnqueueRequest describes a task to be enqueued.
type EnqueueRequest struct {
	// IdempotencyID makes delivery idempotent. Reusing an existing ID returns
	// the original task instead of inserting a duplicate.
	IdempotencyID string
	Payload       []byte
	MaxAttempts   int
	// Delay defers the first claim until now+Delay.
	Delay time.Duration
}

// Lease is handed to a consumer after a successful claim.
type Lease struct {
	TaskID       string
	Attempt      int
	FencingToken int64
	Payload      []byte
	LeaseExpires time.Time
}

// Result reports the outcome of processing a lease.
type Result struct {
	TaskID string
	// FencingToken must match the token on the lease being reported.
	FencingToken int64
	Kind         FailureKind
	Message      string
	Cause        error
}

// Stats is a snapshot of queue counters.
type Stats struct {
	Pending   int
	Running   int
	Succeeded int
	Dead      int
	Total     int
}
