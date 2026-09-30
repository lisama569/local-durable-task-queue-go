package taskqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Config 是本地任务队列的必要配置。
type Config struct {
	// Concurrency 并发消费者数量，默认 1。
	Concurrency int
	// LeaseDuration 一次领取的租约时长；超时未确认视为崩溃，任务被回收。
	LeaseDuration time.Duration
	// MaxAttempts 默认最大尝试次数，默认 3；投递时可按任务覆盖。
	MaxAttempts int
	// RetryDelay 可重试失败后的默认延迟重试间隔。
	RetryDelay time.Duration
	// PollInterval 消费者轮询到期任务的间隔，默认 50ms。
	PollInterval time.Duration
	// RecoveryInterval 过期租约回收扫描间隔，默认 1s。
	RecoveryInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 5 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryDelay < 0 {
		c.RetryDelay = 0
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 50 * time.Millisecond
	}
	if c.RecoveryInterval <= 0 {
		c.RecoveryInterval = time.Second
	}
	return c
}

// Queue 是可嵌入的本地任务队列门面：投递、领取、确认、失败、恢复。
type Queue struct {
	cfg   Config
	store Store
	now   func() time.Time

	mu     sync.Mutex
	closed bool
	seq    atomic.Uint64
}

// New 在内存存储上创建本地队列；进程内嵌入使用，不依赖任何外部中间件。
func New(cfg Config) *Queue {
	return newQueue(cfg, newMemoryStore(), time.Now)
}

// newQueue 注入存储与时钟，供包内测试精确控制时间。
func newQueue(cfg Config, st Store, now func() time.Time) *Queue {
	return &Queue{
		cfg:   cfg.withDefaults(),
		store: st,
		now:   now,
	}
}

// Enqueue 投递任务（幂等），返回任务当前快照。
// 相同幂等键的重复投递会返回已存在的任务与 ErrAlreadyExists（错误需用 errors.Is 判断），
// 不会产生第二个任务，也不会把终态任务重新激活。
func (q *Queue) Enqueue(ctx context.Context, opt EnqueueOptions) (*Task, error) {
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = q.cfg.MaxAttempts
	}
	return q.store.Create(ctx, opt, q.now())
}

// Lease 是一次成功领取的凭据；消费结束必须且只能调用 Ack 或 Nack 一次。
type Lease struct {
	Task     *Task
	q        *Queue
	workerID string

	mu       sync.Mutex
	finished bool
}

// Fetch 并发安全地领取一个到期任务；无任务时返回 (nil, nil)。
// 底层领取是原子的，多个并发消费者绝不会拿到同一个任务的有效租约。
func (q *Queue) Fetch(ctx context.Context, workerID string) (*Lease, error) {
	t, err := q.store.Acquire(ctx, workerID, q.cfg.LeaseDuration, q.now())
	if err != nil || t == nil {
		return nil, err
	}
	return &Lease{Task: t, q: q, workerID: workerID}, nil
}

// Ack 完成确认。重复确认、租约过期或租约属于他人时返回明确错误：
// 已成功的任务不会被再次确认或再次执行。
func (l *Lease) Ack(ctx context.Context, result string) (*Task, error) {
	l.mu.Lock()
	if l.finished {
		t := l.Task
		l.mu.Unlock()
		return t, ErrTerminal
	}
	l.finished = true
	l.mu.Unlock()

	t, err := l.q.store.Complete(ctx, l.Task.ID, l.workerID, result, l.q.now())
	if err != nil {
		// 底层拒绝（如租约恰好过期）时解除本地完成标记，使语义与存储一致：
		// 该租约已失效，任务可能已被回收并由他人执行。
		l.mu.Lock()
		l.finished = false
		l.mu.Unlock()
	} else {
		l.Task = t
	}
	return t, err
}

// Nack 回报失败，使用队列默认的重试延迟。execErr 为 nil 时按可重试错误处理。
func (l *Lease) Nack(ctx context.Context, execErr *ExecError) (*Task, error) {
	return l.NackWithDelay(ctx, execErr, l.q.cfg.RetryDelay)
}

// NackWithDelay 回报失败并指定本次的延迟重试间隔（仅对“将被重试”的任务生效）。
func (l *Lease) NackWithDelay(ctx context.Context, execErr *ExecError, delay time.Duration) (*Task, error) {
	l.mu.Lock()
	if l.finished {
		t := l.Task
		l.mu.Unlock()
		return t, ErrTerminal
	}
	l.finished = true
	l.mu.Unlock()

	t, err := l.q.store.Fail(ctx, l.Task.ID, l.workerID, execErr, delay, l.q.now())
	if err != nil {
		l.mu.Lock()
		l.finished = false
		l.mu.Unlock()
	} else {
		l.Task = t
	}
	return t, err
}

// Get 查询任务（含死信）。
func (q *Queue) Get(ctx context.Context, id TaskID) (*Task, error) {
	return q.store.Get(ctx, id)
}

// GetByIdempotencyKey 按幂等键查询任务。
func (q *Queue) GetByIdempotencyKey(ctx context.Context, key string) (*Task, error) {
	return q.store.GetByIdempotencyKey(ctx, key)
}

// ListDead 列出可查询的死信任务及其完整失败原因链。
func (q *Queue) ListDead(ctx context.Context) ([]*Task, error) {
	return q.store.ListDead(ctx)
}

// RecoverNow 立即执行一次过期租约回收。
// 进程重启后应主动调用一次：确认前崩溃而遗留在 leased 的任务会被重新置为 pending。
func (q *Queue) RecoverNow(ctx context.Context) (int, error) {
	return q.store.RecoverExpiredLeases(ctx, q.now())
}

// Close 停止队列并释放资源。
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.store.Close()
}
