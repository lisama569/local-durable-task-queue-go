# local-durable-task-queue-go

一个**可嵌入 Go 进程**的本地任务队列：支持任务投递、并发独占领取、完成确认 / 失败回报、
自动重试与延迟重试、最大尝试次数与死信，以及进程异常退出后的恢复与幂等处理。

- 不依赖任何外部中间件、外部服务、网络或真实凭据；默认存储为进程内内存实现（`memoryStore`），
  仅用标准库，`go test ./...` 即可完整验证。
- 所有状态迁移在存储层单锁临界区内原子完成；下游用**本地脚本执行器**（`ScriptedExecutor`）
  确定性地模拟成功、可重试失败、不可重试失败以及“执行中进程崩溃”。

> 包名：`taskqueue`，模块路径：`github.com/lisama569/local-durable-task-queue-go`，Go 1.25。

## 1. 快速上手

```go
q := taskqueue.New(taskqueue.Config{
    Concurrency:      4,
    LeaseDuration:    10 * time.Second,
    MaxAttempts:      3,
    RetryDelay:       200 * time.Millisecond,
})
defer q.Close()

// 进程启动时先恢复上一次崩溃遗留在 leased 状态的任务
n, _ := q.RecoverNow(ctx)

t, err := q.Enqueue(ctx, taskqueue.EnqueueOptions{
    IdempotencyKey: "order-123-payment", // 重复投递的幂等依据
    Payload:        `{"order":"123"}`,
    MaxAttempts:    5,                   // 可选，覆盖全局默认
    Delay:          10 * time.Second,    // 可选，延迟投递
})

// 自动消费循环（Fetch -> Execute -> Ack/Nack + 过期租约回收）
g := taskqueue.StartWorkers(ctx, q, myExecutor, 4)
defer g.Stop()
```

手动领取模式：

```go
lease, err := q.Fetch(ctx, "worker-1") // 无任务时返回 (nil, nil)
result, err := myExecutor.Execute(ctx, lease.Task)
if err == nil {
    lease.Ack(ctx, result)
} else {
    lease.Nack(ctx, err.(*taskqueue.ExecError)) // 或 NackWithDelay 指定本次重试延迟
}
```

下游通过返回结构化错误区分失败类别：

```go
return "", &taskqueue.ExecError{Class: taskqueue.ClassRetryable, Reason: "timeout, try later"}
return "", &taskqueue.ExecError{Class: taskqueue.ClassFatal,     Reason: "payload invalid"}
```

## 2. 状态机与合法迁移

任务只有四个状态，所有迁移都在存储层原子完成，非法迁移一律返回显式错误，绝不静默：

```
                 Enqueue
   (不存在) ───────────────▶ pending
                               │  Fetch（到期，AvailableAt <= now）
                               ▼
                            leased ──Ack（租约有效+持有者匹配）──▶ succeeded(终态)
                               │
                               │  Nack 可重试 且 attempt < MaxAttempts
                               ├──────────────▶ pending（AvailableAt = now+RetryDelay）
                               │
                               │  Nack 不可重试(ClassFatal)
                               │  或 attempt >= MaxAttempts（重试预算耗尽）
                               └──────────────▶ dead(终态，可查询 ListDead)

   leased 且 LeaseExpiresAt <= now（消费者崩溃/超时，未 Ack/Nack）
                               └──RecoverExpiredLeases──▶ pending（立即可领取）
```

- `pending → leased → succeeded` 是正常成功路径；`succeeded` / `dead` 为**终态，不可重新激活**。
- 终态任务上再次 Ack/Nack 返回 `ErrTerminal`；过期或非持有者操作返回 `ErrLeaseNotHeld`。
- 任何路径都不会把终态任务改回 pending/leased，恢复扫描只动“已过期的 leased”任务。
- 每次 `Fetch` 都会递增 `attempt` 并登记一条 `AttemptRecord{Attempt, StartedAt}`，
  Ack/Nack/回收时补全 `EndedAt / Class / Reason`，形成可解释失败过程的**原因链**
  （`Task.Attempts`）。崩溃回收的记录原因为
  `lease expired before ack: worker crashed or timed out`。

## 3. 重试与死信规则

| 失败情形 | 处理 |
|---|---|
| 成功（`nil` error） | 立即 Ack → `succeeded`，写入结果 |
| `ClassRetryable` 且 `attempt < MaxAttempts` | 回到 `pending`，`AvailableAt = now + RetryDelay`（延迟重试） |
| `ClassRetryable` 且 `attempt == MaxAttempts` | 进入 `dead`，保留全部尝试记录 |
| `ClassFatal`（不可重试） | **首次失败立即**进入 `dead`，不消耗剩余重试预算 |
| Nack 传 `nil` 错误 | 按可重试错误处理 |

- `MaxAttempts` 可全局配置（默认 3），也可在 `EnqueueOptions.MaxAttempts` 按任务覆盖。
- 死信可通过 `Queue.ListDead(ctx)` 查询，包含负载、尝试次数和完整原因链，便于人工/程序介入。
- **崩溃消耗一次尝试次数**：任务被领取（attempt 递增）后若进程被杀，租约过期回收时该次尝试
  已计入预算。这样“每次领取必执行”的崩溃重试不会无限重试；反复崩溃达到 `MaxAttempts` 后
  同样进入 `dead`，避免毒消息永久卡死队列。

## 4. 并发语义：同一时刻只有一个有效执行者

- 存储用一把互斥锁串行化全部状态迁移。`Acquire` 在同一临界区内完成“挑选到期 pending 任务
  → 置 leased → 写 owner 与租约到期时间”，因此 N 个并发 `Fetch` 绝不会把同一任务发给两个消费者
  （`TestConcurrentFetchIsExclusive` 用 50 消费者 / 50 任务断言一一对应；
  `TestConcurrentStressNoLossNoDuplicate` 用 100 任务 / 8 worker 断言无丢失、无超次执行）。
- Ack/Nack 同时校验 **状态 + owner + 租约时间**：租约过期后原持有者的迟到确认会被拒绝
  （`ErrLeaseNotHeld`），不会覆盖已经被他人重新执行的结果。
- `Lease` 自身还有完成标记：同一租约上重复 Ack/Nack 返回 `ErrTerminal`。
- 返回给调用方的 `*Task` 均为深拷贝快照，外部无法绕过锁篡改内部状态。

## 5. 重启恢复与幂等语义

**租约即崩溃检测信号**：`LeaseDuration` 内未收到 Ack/Nack，即视为消费者死亡。

1. 新进程启动后调用 `Queue.RecoverNow(ctx)`（或由 `WorkerGroup` 按 `RecoveryInterval` 周期执行）。
2. 所有 `leased` 且租约已过期的任务被原子重置为 `pending`、清空 owner、立即可重新领取；
   未过期的租约不动，避免误杀仍在执行的消费者。
3. 已 `succeeded` 的任务结果保持稳定：重复投递（同一 `IdempotencyKey`）返回原任务和
   `ErrAlreadyExists`，不会产生第二个任务，也不会把终态任务再次执行；
   重复 Ack 返回 `ErrTerminal`。
4. 模拟方式：测试中“执行中崩溃”= 已调用执行器但不做 Ack/Nack，推进时钟越过租约期后执行恢复；
   “整组进程重启”见 `TestWorkerGroupCrashAndRestartContinuesTask`（取消 context 杀死 worker 组，
   再启动新组，任务被回收并最终成功，结果稳定）。

**投递幂等**：`IdempotencyKey` 相同的重复投递在锁内完成“检查 + 写入”，返回首次任务快照与
`errors.Is(err, ErrAlreadyExists)`。空幂等键的匿名任务每次创建新 ID。

## 6. 配置项（`taskqueue.Config`）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Concurrency` | 1 | `StartWorkers` 启动的消费者数量 |
| `LeaseDuration` | 5s | 租约时长；超时未确认视为崩溃，任务回收 |
| `MaxAttempts` | 3 | 默认最大尝试次数（投递时可覆盖）；≤0 用默认 |
| `RetryDelay` | 0 | 可重试失败后的延迟重试间隔 |
| `PollInterval` | 50ms | 消费者无任务时的轮询间隔 |
| `RecoveryInterval` | 1s | 过期租约回收扫描间隔 |

注意：租约时长应大于单个任务的正常执行时间；执行时间可能超过租约时应调大租约
（当前版本未提供租约续约，长任务请使用足够长的 `LeaseDuration`）。

## 7. 本地验证方法

无需任何外部依赖，直接运行：

```bash
go build ./...
go vet ./...
go test ./... -race -count=1 -v
```

测试覆盖（测试日志会打印任务标识、尝试次数与判定依据）：

| 测试 | 覆盖点 |
|---|---|
| `TestEnqueueFetchAckHappyPath` | 投递、领取、Ack、重复 Ack、重复投递幂等 |
| `TestConcurrentFetchIsExclusive` | 50 并发消费者独占领取；非持有者 Ack 被拒 |
| `TestRetryableThenSuccess` | 可重试失败、延迟窗口内不可领取、延迟到期后成功 |
| `TestFatalFailureGoesStraightToDead` | 不可重试失败直接死信；死信不可再投递激活 |
| `TestRetryExhaustionDeadLettersWithReasonChain` | 重试超限死信；尝试次数与原因链完整可查 |
| `TestDelayNotExpiredBlocksFetch` | 延迟投递 / `AvailableAt` 边界 |
| `TestRecoverLeasedTaskAfterCrashBeforeAck` | 确认前崩溃、租约过期回收、继续推进、终态稳定 |
| `TestRecoveryDoesNotTouchTerminalTasks` | 恢复不激活任何终态任务；终态 Ack/Nack 被拒 |
| `TestWorkerGroupEndToEnd` | 4 worker 混合场景全部到达正确终态、不丢不卡 |
| `TestWorkerGroupCrashAndRestartContinuesTask` | 整组 worker 被杀死后“重启”，任务恢复并成功 |
| `TestConcurrentStressNoLossNoDuplicate` | 100 任务 × 8 worker：无丢失、无超次/重复执行 |

## 8. 文件结构

| 文件 | 职责 |
|---|---|
| `task.go` | `Task` / `Status` / `AttemptRecord` 类型 |
| `errors.go` | `ErrorClass`、`ExecError` 与哨兵错误 |
| `store.go` | `Store` 接口 + 并发安全内存存储（状态机边界所在） |
| `queue.go` | `Queue` 门面、配置、幂等投递、`Lease` 的 Ack/Nack |
| `executor.go` | `Executor` 接口与 `ScriptedExecutor` 本地可控下游 |
| `worker.go` | 并发消费者循环与过期租约回收循环 |
