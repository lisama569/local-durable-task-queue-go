package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，让状态机测试不依赖真实等待。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestQueue(c *fakeClock, cfg Config) *Queue {
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = time.Minute
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	return newQueue(cfg, newMemoryStore(), c.now)
}

func logTaskState(t *testing.T, label string, task *Task) {
	t.Helper()
	if task == nil {
		t.Logf("%s: <nil task>", label)
		return
	}
	t.Logf("%s: task=%s idempotencyKey=%q status=%s attempt=%d/%d owner=%q attempts=%d",
		label, task.ID, task.IdempotencyKey, task.Status, task.Attempt, task.MaxAttempts,
		task.LeaseOwner, len(task.Attempts))
}

func TestEnqueueFetchAckHappyPath(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{})

	created, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "k-1", Payload: "do-work"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	logTaskState(t, "enqueued", created)

	lease, err := q.Fetch(ctx, "w1")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if lease == nil {
		t.Fatal("expected to acquire a task, got nil lease")
	}
	if lease.Task.ID != created.ID || lease.Task.Attempt != 1 || lease.Task.Status != StatusLeased {
		t.Fatalf("unexpected lease task: %+v", lease.Task)
	}
	logTaskState(t, "fetched", lease.Task)

	done, err := lease.Ack(ctx, "result-42")
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if done.Status != StatusSucceeded || done.Result != "result-42" {
		t.Fatalf("unexpected ack result: %+v", done)
	}
	logTaskState(t, "acked", done)

	// 断言 1：成功任务不能再被领取。
	again, err := q.Fetch(ctx, "w2")
	if err != nil {
		t.Fatalf("fetch after ack: %v", err)
	}
	if again != nil {
		t.Fatalf("succeeded task must not be re-delivered, got %+v", again.Task)
	}

	// 断言 2：重复确认得到明确错误（ErrTerminal），而非静默成功。
	_, err = lease.Ack(ctx, "result-again")
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("duplicate ack expected ErrTerminal, got %v", err)
	}
	t.Logf("duplicate ack correctly rejected: %v (basis: terminal state guard)", err)

	// 断言 3：重复投递同一幂等键返回原任务且不重新激活。
	dup, err := q.Enqueue(ctx, EnqueueOptions{IdempotencyKey: "k-1", Payload: "do-work"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate enqueue expected ErrAlreadyExists, got %v", err)
	}
	if dup.ID != created.ID || dup.Status != StatusSucceeded {
		t.Fatalf("duplicate enqueue must return original succeeded task, got %+v", dup)
	}
	t.Logf("duplicate enqueue returned original task=%s status=%s (basis: idempotency key)", dup.ID, dup.Status)
}

func TestConcurrentFetchIsExclusive(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	q := newTestQueue(clk, Config{})

	const n = 50
	ids := make(map[TaskID]bool)
	for i := 0; i < n; i++ {
		task, err := q.Enqueue(ctx, EnqueueOptions{
			IdempotencyKey: fmt.Sprintf("key-%d", i),
			Payload:        "p",
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		ids[task.ID] = true
	}

	// 50 个消费者同时领取 50 个任务：每个任务恰好被一个消费者拿到。
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[TaskID]string)
	barrier := make(chan struct{})
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			workerID := fmt.Sprintf("w-%d", w)
			<-barrier
			lease, err := q.Fetch(ctx, workerID)
			if err != nil || lease == nil {
				return
			}
			mu.Lock()
			seen[lease.Task.ID] = workerID
			mu.Unlock()
		}(w)
	}
	close(barrier)
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("exclusivity violated: %d unique tasks acquired, expected %d", len(seen), n)
	}
	for id := range seen {
		if !ids[id] {
			t.Fatalf("unknown task acquired: %s", id)
		}
	}
	t.Logf("concurrent fetch basis: %d workers -> %d distinct tasks, each owned by exactly one worker", n, len(seen))

	// 全部被领取后，队列中应无可领取任务。
	extra, err := q.Fetch(ctx, "late-worker")
	if err != nil || extra != nil {
		t.Fatalf("expected no more tasks, got lease=%v err=%v", extra, err)
	}

	// 他人持有的租约不能确认。
	sampleID := func() TaskID {
		for id := range seen {
			return id
		}
		return ""
	}()
	owner := seen[sampleID]
	task, _ := q.Get(ctx, sampleID)
	lease := &Lease{Task: task, q: q, workerID: "intruder"}
	if _, err := lease.Ack(ctx, "hijack"); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("intruder ack expected ErrLeaseNotHeld, got %v (rightful owner=%s)", err, owner)
	}
	t.Logf("task=%s owner-only ack enforced; owner=%s, intruder rejected with ErrLeaseNotHeld", sampleID, owner)
}
