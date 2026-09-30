package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentClaimExclusive(t *testing.T) {
	const n = 25
	q, ctx := newTestQueue(t)
	for i := 0; i < n; i++ {
		mustEnqueue(t, q, ctx, fmt.Sprintf("job-%02d", i), nil)
	}

	const consumers = 8
	claims := make(chan *Lease, n)
	var wg sync.WaitGroup
	cctx, cancel := context.WithCancel(ctx)
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lctx, lcancel := context.WithTimeout(cctx, 50*time.Millisecond)
				l, err := q.Claim(lctx)
				lcancel()
				if err != nil {
					return
				}
				select {
				case claims <- l:
				case <-cctx.Done():
					return
				}
			}
		}()
	}

	got := make(map[string]int)
	deadline := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case l := <-claims:
			if _, dup := got[l.TaskID]; dup {
				t.Fatalf("task=%s claimed twice concurrently (judgement: one effective executor per task)", l.TaskID)
			}
			got[l.TaskID] = l.Attempt
		case <-deadline:
			t.Fatalf("only claimed %d/%d tasks (judgement: no task may stall forever)", len(got), n)
		}
	}
	cancel()
	wg.Wait()

	if len(got) != n {
		t.Fatalf("unique claims=%d want=%d", len(got), n)
	}
	for id, attempt := range got {
		t.Logf("claimed task=%s attempt=%d (judgement: unique owner)", id, attempt)
	}
	stats, _ := q.Stats(ctx)
	t.Logf("stats pending=%d running=%d succeeded=%d dead=%d total=%d (judgement: all n leased once)",
		stats.Pending, stats.Running, stats.Succeeded, stats.Dead, stats.Total)
	if stats.Running != n {
		t.Fatalf("running=%d want=%d", stats.Running, n)
	}
}

func TestDuplicateCompleteAndStaleReports(t *testing.T) {
	q, ctx := newTestQueue(t)
	task := mustEnqueue(t, q, ctx, "once", nil)
	lease := claimWithin(t, q, time.Second)

	// Stale token (older generation) must be rejected.
	if err := q.Complete(ctx, task.ID, lease.FencingToken-1); !errors.Is(err, ErrFencing) {
		t.Fatalf("complete with stale token err=%v want ErrFencing", err)
	}
	t.Logf("task=%s attempt=%d stale token %d rejected (judgement: expired lease cannot confirm)",
		task.ID, lease.Attempt, lease.FencingToken-1)

	if err := q.Complete(ctx, task.ID, lease.FencingToken); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	// Second confirm with the same token is a harmless idempotent no-op...
	if err := q.Complete(ctx, task.ID, lease.FencingToken); err != nil {
		t.Fatalf("duplicate complete with same token: %v", err)
	}
	// ...and a confirm with a different token is rejected: terminal task
	// can never be re-activated.
	if err := q.Complete(ctx, task.ID, lease.FencingToken+1); !errors.Is(err, ErrTerminal) {
		t.Fatalf("complete terminal task err=%v want ErrTerminal", err)
	}
	// A failure report on a succeeded task is rejected too.
	err := q.Fail(ctx, Result{TaskID: task.ID, FencingToken: lease.FencingToken,
		Kind: FailureKindRetryable, Message: "late report"})
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("fail on succeeded task err=%v want ErrTerminal", err)
	}
	done := assertState(t, q, ctx, task.ID, StateSucceeded)
	t.Logf("task=%s state=%s after duplicate/late reports (judgement: confirmed result is stable)",
		task.ID, done.State)
}

func TestExpiredLeaseIsReclaimedAndOldTokenRejected(t *testing.T) {
	q, clock := newClockQueue(t, Config{WorkerID: "w1"})
	ctx := context.Background()

	task := mustEnqueue(t, q, ctx, "expiry", nil)
	lease := claimWithin(t, q, time.Second)
	t.Logf("task=%s leased token=%d expires=%s", task.ID, lease.FencingToken, lease.LeaseExpires.Format(time.RFC3339Nano))

	// Worker dies without reporting; TTL passes. Advance the fake clock.
	clock.advance(2 * time.Second)
	second := claimWithin(t, q, time.Second)
	if second.TaskID != task.ID {
		t.Fatalf("reclaimed wrong task=%s want=%s", second.TaskID, task.ID)
	}
	if second.FencingToken <= lease.FencingToken {
		t.Fatalf("new token=%d must exceed old=%d", second.FencingToken, lease.FencingToken)
	}
	t.Logf("task=%s reclaimed after ttl attempt=%d token=%d (judgement: no permanent stall)",
		task.ID, second.Attempt, second.FencingToken)

	// Old owner wakes up and tries to confirm/fail: both rejected.
	if err := q.Complete(context.Background(), task.ID, lease.FencingToken); !errors.Is(err, ErrFencing) {
		t.Fatalf("old owner complete err=%v want ErrFencing", err)
	}
	if err := q.Fail(ctx, Result{TaskID: task.ID, FencingToken: lease.FencingToken,
		Kind: FailureKindRetryable, Message: "zombie report"}); !errors.Is(err, ErrFencing) {
		t.Fatalf("old owner fail err=%v want ErrFencing", err)
	}
	if err := q.Complete(ctx, task.ID, second.FencingToken); err != nil {
		t.Fatalf("new owner complete: %v", err)
	}
	assertState(t, q, ctx, task.ID, StateSucceeded)
	t.Logf("task=%s confirmed by new owner attempt=%d; zombie reports ignored (judgement: result not duplicated)",
		task.ID, second.Attempt)
}
