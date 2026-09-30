package taskqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// EnqueueOptions 描述一次任务投递的参数。
type EnqueueOptions struct {
	// ID 任务标识；为空时由队列生成。
	ID TaskID
	// IdempotencyKey 幂等键；相同键的重复投递不会产生第二个任务。
	IdempotencyKey string
	Payload        string
	MaxAttempts    int
	// Delay 延迟投递/延迟重试的等待时长。
	Delay time.Duration
}

// Store 抽象任务的持久化与状态迁移，所有实现必须保证并发安全。
type Store interface {
	// Create 按幂等键创建任务；键已存在时返回已存在的任务与 ErrAlreadyExists。
	Create(ctx context.Context, opt EnqueueOptions, now time.Time) (*Task, error)
	// Acquire 原子领取一个到期可执行的任务并写入租约。
	Acquire(ctx context.Context, workerID string, leaseFor time.Duration, now time.Time) (*Task, error)
	// Complete 在租约仍有效时把任务迁移到 succeeded。
	Complete(ctx context.Context, id TaskID, workerID string, result string, now time.Time) (*Task, error)
	// Fail 在租约仍有效时回报失败：重试则回到 pending（可带延迟），否则进入 dead。
	Fail(ctx context.Context, id TaskID, workerID string, ferr *ExecError, retryDelay time.Duration, now time.Time) (*Task, error)
	// Get 按 ID 查询任务（含死信任务）。
	Get(ctx context.Context, id TaskID) (*Task, error)
	// GetByIdempotencyKey 按幂等键查询任务。
	GetByIdempotencyKey(ctx context.Context, key string) (*Task, error)
	// RecoverExpiredLeases 把过期未续约、未确认的任务回收为 pending，供重启/崩溃恢复。
	RecoverExpiredLeases(ctx context.Context, now time.Time) (int, error)
	// ListDead 返回死信队列中的任务快照。
	ListDead(ctx context.Context) ([]*Task, error)
	// Close 释放底层资源。
	Close() error
}

// defaultMaxAttempts 是投递时未显式指定 MaxAttempts 的兜底值。
const defaultMaxAttempts = 3

// memoryStore 是基于互斥锁 + map 的内存存储。
//
// 单把互斥锁串行化所有状态迁移，保证“领取即占用”的原子性：
// 并发的 Acquire 绝不会把同一个任务发给两个消费者。Create 的幂等检查与
// 写入也在同一临界区内完成，因此重复投递不会产生重复任务。
type memoryStore struct {
	mu    sync.Mutex
	tasks map[TaskID]*Task
	byKey map[string]TaskID
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tasks: make(map[TaskID]*Task),
		byKey: make(map[string]TaskID),
	}
}

func newTaskID() TaskID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return TaskID(hex.EncodeToString(b[:]))
}

// cloneTask 返回任务的深拷贝快照，避免调用方在锁外篡改内部状态。
func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Attempts != nil {
		cp.Attempts = append([]AttemptRecord(nil), t.Attempts...)
	}
	return &cp
}

func (s *memoryStore) Create(ctx context.Context, opt EnqueueOptions, now time.Time) (*Task, error) {
	if opt.IdempotencyKey == "" && opt.ID == "" {
		// 无任何去重依据时仍允许匿名任务，但至少要有负载。
		if opt.Payload == "" {
			return nil, ErrInvalidArgument
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 幂等键命中：无论任务处于何种状态（含终态），都返回原任务，
	// 已确认任务绝不会因重复投递而被再次执行。
	if opt.IdempotencyKey != "" {
		if id, ok := s.byKey[opt.IdempotencyKey]; ok {
			return cloneTask(s.tasks[id]), ErrAlreadyExists
		}
	}

	id := opt.ID
	if id == "" {
		id = newTaskID()
		// 极小概率碰撞时重新生成。
		for _, exists := s.tasks[id]; exists; {
			id = newTaskID()
		}
	} else if _, exists := s.tasks[id]; exists {
		return nil, ErrAlreadyExists
	}

	maxAttempts := opt.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	t := &Task{
		ID:             id,
		IdempotencyKey: opt.IdempotencyKey,
		Payload:        opt.Payload,
		Status:         StatusPending,
		Attempt:        0,
		MaxAttempts:    maxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
		AvailableAt:    now.Add(opt.Delay),
	}
	if opt.IdempotencyKey != "" {
		s.byKey[opt.IdempotencyKey] = id
	}
	s.tasks[id] = t
	return cloneTask(t), nil
}

func (s *memoryStore) Acquire(ctx context.Context, workerID string, leaseFor time.Duration, now time.Time) (*Task, error) {
	if workerID == "" {
		return nil, ErrInvalidArgument
	}
	if leaseFor <= 0 {
		return nil, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 选择一个已到期的 pending 任务；map 遍历顺序随机，天然在消费者间分散负载。
	var chosen *Task
	for _, t := range s.tasks {
		if t.Status != StatusPending {
			continue
		}
		if now.Before(t.AvailableAt) {
			continue // 延迟重试尚未到期
		}
		if chosen == nil || t.AvailableAt.Before(chosen.AvailableAt) {
			chosen = t
		}
	}
	if chosen == nil {
		return nil, nil
	}

	chosen.Status = StatusLeased
	chosen.Attempt++
	chosen.LeaseOwner = workerID
	chosen.LeaseExpiresAt = now.Add(leaseFor)
	chosen.UpdatedAt = now
	// 领取即开启一次新尝试并登记开始时间；确认/失败/回收都会补全这条记录。
	chosen.Attempts = append(chosen.Attempts, AttemptRecord{
		Attempt:   chosen.Attempt,
		StartedAt: now,
	})
	return cloneTask(chosen), nil
}

func (s *memoryStore) Complete(ctx context.Context, id TaskID, workerID string, result string, now time.Time) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	// 终态保护 + 租约校验：只有当前持有者、在租约有效期内才能确认成功。
	if t.terminal() {
		return cloneTask(t), ErrTerminal
	}
	if t.Status != StatusLeased || t.LeaseOwner != workerID || !now.Before(t.LeaseExpiresAt) {
		return cloneTask(t), ErrLeaseNotHeld
	}

	t.Status = StatusSucceeded
	t.Result = result
	t.LeaseOwner = ""
	t.LeaseExpiresAt = time.Time{}
	t.UpdatedAt = now
	if n := len(t.Attempts); n > 0 {
		t.Attempts[n-1].EndedAt = now
	}
	return cloneTask(t), nil
}

func (s *memoryStore) Fail(ctx context.Context, id TaskID, workerID string, ferr *ExecError, retryDelay time.Duration, now time.Time) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	if t.terminal() {
		return cloneTask(t), ErrTerminal
	}
	if t.Status != StatusLeased || t.LeaseOwner != workerID || !now.Before(t.LeaseExpiresAt) {
		return cloneTask(t), ErrLeaseNotHeld
	}

	if ferr == nil {
		ferr = &ExecError{Class: ClassRetryable, Reason: "execution failed without error detail"}
	}
	rec := AttemptRecord{
		Attempt: t.Attempt,
		EndedAt: now,
		Class:   ferr.Class,
		Reason:  ferr.Reason,
	}
	if n := len(t.Attempts); n > 0 {
		rec.StartedAt = t.Attempts[n-1].StartedAt
		t.Attempts[n-1] = rec
	} else {
		t.Attempts = append(t.Attempts, rec)
	}

	// 不可重试错误立即死信；可重试错误超过最大尝试次数同样进入死信。
	if ferr.Class == ClassFatal || t.Attempt >= t.MaxAttempts {
		t.Status = StatusDead
		t.AvailableAt = now
	} else {
		t.Status = StatusPending
		t.AvailableAt = now.Add(retryDelay)
	}
	t.LeaseOwner = ""
	t.LeaseExpiresAt = time.Time{}
	t.UpdatedAt = now
	return cloneTask(t), nil
}

func (s *memoryStore) Get(ctx context.Context, id TaskID) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneTask(t), nil
}

func (s *memoryStore) GetByIdempotencyKey(ctx context.Context, key string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byKey[key]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneTask(s.tasks[id]), nil
}

func (s *memoryStore) RecoverExpiredLeases(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, t := range s.tasks {
		// 仅回收“租约已过期”的 leased 任务：这表示领取它的消费者在确认前
		// 崩溃/被杀。立即重新置为 pending 且立即可领取，终态任务不受影响。
		if t.Status == StatusLeased && !now.Before(t.LeaseExpiresAt) {
			// 补全被中断的那次尝试，留下可追踪的原因链。
			if n := len(t.Attempts); n > 0 {
				t.Attempts[n-1].EndedAt = t.LeaseExpiresAt
				t.Attempts[n-1].Class = ClassRetryable
				t.Attempts[n-1].Reason = "lease expired before ack: worker crashed or timed out"
			}
			t.Status = StatusPending
			t.AvailableAt = now
			t.LeaseOwner = ""
			t.LeaseExpiresAt = time.Time{}
			t.UpdatedAt = now
			n++
		}
	}
	return n, nil
}

func (s *memoryStore) ListDead(ctx context.Context) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Task
	for _, t := range s.tasks {
		if t.Status == StatusDead {
			out = append(out, cloneTask(t))
		}
	}
	return out, nil
}

func (s *memoryStore) Close() error {
	return nil
}
