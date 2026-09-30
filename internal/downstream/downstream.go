// Package downstream is a local, controllable stand-in for a downstream
// service. It simulates success, retryable failure, permanent failure and
// attempt-based behavior using in-memory scripted state: no network, no
// external service and no credentials.
package downstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lisama569/local-durable-task-queue-go/taskqueue"
)

// Outcome describes what happens on one execution attempt.
type Outcome int

const (
	// Succeed makes the attempt return nil.
	Succeed Outcome = iota
	// FailRetryable makes the attempt return a RetryableError.
	FailRetryable
	// FailPermanent makes the attempt return a non-retryable error.
	FailPermanent
)

// Script is a per-key plan. Outcomes[attempt-1] is used for that attempt
// number; attempts beyond the slice use Default.
type Script struct {
	// Outcomes by (1-based) attempt index.
	Outcomes []Outcome
	// Messages[i] optionally annotates Outcomes[i] with a failure reason.
	Messages []string
	// Default applies to attempts after Outcomes is exhausted (default Succeed).
	Default Outcome
}

// Payload is the JSON payload understood by the Simulator.
//
//	{"key":"charge-card-123"}
//
// Tasks without a payload use their task ID as key.
type Payload struct {
	Key string `json:"key"`
}

// Simulator executes scripts and records every attempted call, so tests can
// assert that a task body ran at most once per valid lease.
type Simulator struct {
	mu      sync.Mutex
	scripts map[string]Script
	calls   []Call
}

// Call records one executor invocation.
type Call struct {
	Key     string
	TaskID  string
	Attempt int
}

// New returns an empty Simulator.
func New() *Simulator {
	return &Simulator{scripts: make(map[string]Script)}
}

// SetScript installs (or replaces) the plan for a key.
func (s *Simulator) SetScript(key string, sc Script) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts[key] = sc
}

// Calls returns a copy of every invocation seen so far.
func (s *Simulator) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	copy(out, s.calls)
	return out
}

// CallsForKey returns invocation attempt numbers for a key in call order.
func (s *Simulator) CallsForKey(key string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var attempts []int
	for _, c := range s.calls {
		if c.Key == key {
			attempts = append(attempts, c.Attempt)
		}
	}
	return attempts
}

// Execute runs the scripted outcome for key, at the given attempt.
func (s *Simulator) Execute(ctx context.Context, taskID string, attempt int, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return taskqueue.Retryable(err, "context canceled before execution")
	}
	key := taskID
	if len(payload) > 0 {
		var p Payload
		if err := json.Unmarshal(payload, &p); err == nil && p.Key != "" {
			key = p.Key
		}
	}

	s.mu.Lock()
	s.calls = append(s.calls, Call{Key: key, TaskID: taskID, Attempt: attempt})
	sc, ok := s.scripts[key]
	s.mu.Unlock()

	out := Succeed
	if ok {
		if attempt >= 1 && attempt <= len(sc.Outcomes) {
			out = sc.Outcomes[attempt-1]
		} else {
			out = sc.Default
		}
	}
	msg := fmt.Sprintf("simulated downstream result key=%s attempt=%d outcome=%d", key, attempt, out)
	if ok && attempt >= 1 && attempt <= len(sc.Messages) && sc.Messages[attempt-1] != "" {
		msg = sc.Messages[attempt-1]
	}
	switch out {
	case FailRetryable:
		return taskqueue.Retryable(fmt.Errorf("simulated transient fault: %s", msg), "downstream timeout")
	case FailPermanent:
		return taskqueue.Permanent(fmt.Errorf("simulated permanent fault: %s", msg), "invalid request")
	default:
		return nil
	}
}
