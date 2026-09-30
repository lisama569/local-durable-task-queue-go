package taskqueue

import "time"

// TaskID 是任务的唯一标识。
type TaskID string

// Status 表示任务所处的生命周期状态。
type Status string

const (
	// StatusPending 已投递、等待被领取（含延迟重试到期前的等待）。
	StatusPending Status = "pending"
	// StatusLeased 已被某个消费者领取，租约有效期内由该消费者独占执行。
	StatusLeased Status = "leased"
	// StatusSucceeded 终态：已完成确认，结果稳定，不可再被激活。
	StatusSucceeded Status = "succeeded"
	// StatusDead 终态：不可重试失败或重试次数耗尽，进入死信，可查询。
	StatusDead Status = "dead"
)

// AttemptRecord 记录一次执行尝试的判定依据，用于追踪失败原因链。
type AttemptRecord struct {
	Attempt   int        `json:"attempt"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   time.Time  `json:"ended_at"`
	Class     ErrorClass `json:"class"`
	Reason    string     `json:"reason"`
}

// Task 是队列持久化与状态迁移的基本单元。
type Task struct {
	ID             TaskID          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        string          `json:"payload"`
	Status         Status          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	AvailableAt    time.Time       `json:"available_at"`
	LeaseOwner     string          `json:"lease_owner"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
	Result         string          `json:"result"`
	Attempts       []AttemptRecord `json:"attempts"`
}

// terminal 报告任务是否已处于不可再迁移的终态。
func (t *Task) terminal() bool {
	return t.Status == StatusSucceeded || t.Status == StatusDead
}
