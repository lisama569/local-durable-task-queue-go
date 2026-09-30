package taskqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRestartRecoversUnconfirmedAndPreservesTerminals(t *testing.T) {
	dir := t.TempDir() + "/queue"
	cfg := Config{
		Dir: dir, WorkerID: "pid-1",
		LeaseTTL:    time.Hour, // expiry must NOT be the reason for recovery
		MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond,
		ClaimPollInterval: time.Millisecond,
	}
	q1, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}
	ctx := context.Background()

	// A: claimed but never confirmed -> simulates crash mid-processing.
	running := mustEnqueue(t, q1, ctx, "crash-mid", nil)
	leaseA, err := q1.Claim(ctx)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}
	// B: still pending, never claimed.
	pending := mustEnqueue(t, q1, ctx, "never-claimed", nil)
	// C: already succeeded.
	succeeded := mustEnqueue(t, q1, ctx, "done", nil)
	leaseC, err := q1.Claim(ctx)
	if err != nil {
		t.Fatalf("claim C: %v", err)
	}
	if err := q1.Complete(ctx, succeeded.ID, leaseC.FencingToken); err != nil {
		t.Fatalf("complete C: %v", err)
	}
	// D: failed permanently -> dead before crash.
	dead := mustEnqueue(t, q1, ctx, "dead-before", nil)
	leaseD, err := q1.Claim(ctx)
	if err != nil {
		t.Fatalf("claim D: %v", err)
	}
	if err := q1.Fail(ctx, Result{
		TaskID: leaseD.TaskID, FencingToken: leaseD.FencingToken,
		Kind: FailureKindPermanent, Message: "bad",
		Cause: Permanent(errors.New("400"), "bad request"),
	}); err != nil {
		t.Fatalf("fail D: %v", err)
	}

	// Abrupt process exit: no graceful shutdown, lease A never released.
	if err := q1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// New process opens the same directory.
	cfg.WorkerID = "pid-2"
	q2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open #2: %v", err)
	}
	defer q2.Close()

	rt := assertState(t, q2, ctx, running.ID, StatePending)
	if rt.Attempt != 1 {
		t.Fatalf("recovered task attempt=%d want 1", rt.Attempt)
	}
	t.Logf("task=%s recovered running->pending after restart attempt=%d token=%d (judgement: unconfirmed work resumes, not lost)",
		running.ID, rt.Attempt, rt.FencingToken)

	assertState(t, q2, ctx, pending.ID, StatePending)
	t.Logf("task=%s remains pending after restart (judgement: queued work survives)", pending.ID)

	st := assertState(t, q2, ctx, succeeded.ID, StateSucceeded)
	t.Logf("task=%s remains %s after restart (judgement: confirmed result is stable)", succeeded.ID, st.State)
	dt := assertState(t, q2, ctx, dead.ID, StateDead)
	if len(dt.Failures) != 1 || dt.Failures[0].Kind != FailureKindPermanent {
		t.Fatalf("dead task chain=%v want one permanent failure", dt.Failures)
	}
	t.Logf("task=%s remains dead after restart chain=%d kind=%s (judgement: terminal not reactivated, reasons kept)",
		dead.ID, len(dt.Failures), dt.Failures[0].Kind)

	// Exactly the two non-terminal tasks are claimable after recovery.
	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		l, err := q2.Claim(ctxT)
		cancel()
		if err != nil {
			t.Fatalf("post-restart claim %d: %v", i, err)
		}
		ids[l.TaskID] = true
	}
	if !ids[running.ID] || !ids[pending.ID] {
		t.Fatalf("post-restart claims=%v want {%s,%s}", ids, running.ID, pending.ID)
	}
	t.Logf("post-restart claims=%v (judgement: only live work re-leased)", ids)

	// The zombie token from the crashed process must be rejected.
	if err := q2.Complete(ctx, leaseA.TaskID, leaseA.FencingToken); !errors.Is(err, ErrFencing) {
		t.Fatalf("zombie complete after restart err=%v want ErrFencing", err)
	}
	t.Logf("task=%s stale token %d from crashed process rejected (judgement: old lease invalidated)",
		leaseA.TaskID, leaseA.FencingToken)
}

func TestRestartReplaysInFlightRetryChain(t *testing.T) {
	dir := t.TempDir() + "/queue"
	mkCfg := func(worker string) Config {
		return Config{
			Dir: dir, WorkerID: worker, LeaseTTL: time.Hour,
			MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
			ClaimPollInterval: time.Millisecond,
		}
	}
	q1, err := Open(context.Background(), mkCfg("pid-1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task := mustEnqueue(t, q1, ctx, "retry-restart", nil)
	l1, err := q1.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q1.Fail(ctx, Result{
		TaskID: l1.TaskID, FencingToken: l1.FencingToken,
		Kind: FailureKindRetryable, Message: "transient-1",
		Cause: Retryable(errors.New("conn reset"), "io"),
	}); err != nil {
		t.Fatal(err)
	}
	// Retry was scheduled (pending+RunAt) when the process exits.
	_ = q1.Close()

	q2, err := Open(context.Background(), mkCfg("pid-2"))
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	t0, err := q2.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if t0.State != StatePending || t0.Attempt != 1 || len(t0.Failures) != 1 {
		t.Fatalf("after restart state=%s attempt=%d failures=%d want pending/1/1",
			t0.State, t0.Attempt, len(t0.Failures))
	}
	if !t0.RunAt.After(t0.CreatedAt) {
		t.Fatalf("delayed RunAt not replayed: %s", t0.RunAt)
	}
	t.Logf("task=%s replayed attempt=%d runAt=%s chainLen=%d chain0=%q (judgement: retry schedule + reason survive restart)",
		task.ID, t0.Attempt, t0.RunAt.Format(time.RFC3339Nano), len(t0.Failures), t0.Failures[0].Cause)

	time.Sleep(3 * time.Millisecond)
	l2, err := q2.Claim(ctx)
	if err != nil {
		t.Fatalf("claim retry after restart: %v", err)
	}
	if l2.Attempt != 2 {
		t.Fatalf("attempt=%d want 2 (budget must be preserved across restart)", l2.Attempt)
	}
	if err := q2.Fail(ctx, Result{
		TaskID: l2.TaskID, FencingToken: l2.FencingToken,
		Kind: FailureKindRetryable, Message: "transient-2",
		Cause: Retryable(errors.New("timeout"), "io"),
	}); err != nil {
		t.Fatal(err)
	}
	t1 := assertState(t, q2, ctx, task.ID, StateDead)
	if len(t1.Failures) != 2 {
		t.Fatalf("chain=%d want 2 across restart (judgement: full failure chain retained)", len(t1.Failures))
	}
	t.Logf("task=%s dead after attempt=%d chain=%d (judgement: max attempts honored across restart)",
		task.ID, t1.Attempt, len(t1.Failures))
}
