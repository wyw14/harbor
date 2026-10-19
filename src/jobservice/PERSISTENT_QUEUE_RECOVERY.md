# Jobservice 持久化队列：故障恢复语义与实现边界

本文描述 jobservice 基于 Redis（gocraft/work）持久化队列与 core 侧数据库（PostgreSQL）
在重启、回调重复、取消、部分提交等场景下的行为契约，并标注新增测试如何锁定这些行为。

相关代码：

- 队列/收割：`src/jobservice/worker/cworker/`（`c_worker.go`、`reaper.go`、`de_duplicator.go`）
- 执行与状态机：`src/jobservice/runner/redis.go`、`src/jobservice/job/tracker.go`、
  `src/jobservice/lcm/controller.go`、`src/jobservice/common/rds/scripts.go`
- 回调投递：`src/jobservice/hook/hook_agent.go`
- core 接收端：`src/pkg/task/hook.go`、`src/pkg/task/dao/task.go`、
  `src/pkg/task/dao/execution.go`、`src/server/handler/job_status_hook.go`
- 运维面：`src/controller/jobmonitor/monitor.go`
- 作业实现：`src/jobservice/job/impl/**`

## 1. 数据模型速览

| 载体 | Key / 表 | 内容 |
|---|---|---|
| 就绪队列 | `<ns>:jobs:<type>` (LIST) | 等待领取的 job JSON |
| 进行中队列 | `<ns>:jobs:<type>:<poolID>:inprogress` (LIST) | worker 已 `BRPOPLPUSH` 领取的 payload |
| 并发锁 | `<ns>:jobs:<type>:lock` / `:lock_info` | 每领取一个 +1，完成/重入队时 -1 |
| 任务状态 | `<ns>:job_stats:<jobID>` (HASH) | status、revision、ack、check_in、web_hook_url、过期时间 |
| 在途跟踪 | `<ns>:job_track:inprogress` (HASH) | jobID → 计数（**初始 2**，终态转换与回调 ACK 各释放 1） |
| unique 标记 | `<ns>:unique:running:<type>:<params>` | `SET NX EX 86400` |
| core 任务表 | `task` | job_id、execution_id、status/status_code、status_revision |
| core 执行表 | `execution` | vendor 维度聚合状态，revision 乐观锁 |

状态码：`Pending=0, Scheduled=1, Running=2, Success/Stopped/Error=3（终态）`。
revision 是**秒级** Unix 时间戳；每次 `Reset()`（重放/重试）重新取当前时间。

## 2. 重启恢复：三类任务如何区分

jobservice 启动时 `reaper.start()` 做两件事：启动 5 分钟后的周期性对账
（`syncOutdatedStats`，之后每小时一次），以及启动 5 秒后的一次性死池回收
（`reEnqueueInProgressJobs`）。

### 2.1 已领取（claimed / in-progress）

1. 死池判定：`SMEMBERS <ns>:worker_pools` 中不存在的 poolID 视为死池。
2. 对每个注册任务类型扫描 `lock_info`，用 Lua（`RedisLuaReenqueueScript`）
   `RPOPLPUSH inprogress → jobs:<type>` 并 `DECR` 锁，循环直到队列为空。
   搬运是原子的，**保持原始 job JSON 与 JobID 不变**。
3. 新 worker 领取后，`runner.RedisJob.Run` 依据 tracker 当前状态分发：
   - `Running`/`Error` → `tracker.Reset()`（状态回 Pending、revision 刷新、
     清除旧 ack/check_in、track 重置为 2）→ **重新执行一次 `Run`**；
   - `Pending`/`Scheduled` → 正常执行；
   - `Stopped` → 直接返回，不执行业务；
   - `Success` → 直接返回，业务逻辑**绝不重复执行**。
4. 当前存活 pool 的 in-progress 队列不会被动（避免把在途任务执行两次）。

> 边界：空死池但 `lock_info` 残留计数时，没有 payload 可搬运，陈旧计数器不会被修正
> （仅成功搬运才释放锁）；该路径不返回错误、不阻断启动。

### 2.2 已完成（completed / final）

- runner 看到 `Success/Stopped/Error` 终态时不再次进入业务 `Run`。
- 周期性对账比较 `status` 与 `ack`（`compare()`，按 revision → 状态码 → check_in_at）：
  - 一致且终态：`HDEL job_track:inprogress` 清理跟踪；
  - status 落后于 ack：用 ack 状态覆盖本地状态（终态覆盖同时释放 track 计数）；
  - status 领先（回调未 ACK）：重新 `FireHook`，由回调 ACK 闭环。
- 终态 stats 带 TTL：Success/Stopped 1 天，Error 7 天（允许重试窗口）。

### 2.3 过期 / 挂死（expired / hung）

status 与 ack 一致但非终态，且 `update_time` 超过 `config.MaxUpdateDuration()`
（默认 24 小时）→ 标记 `Error` 并触发 Error 回调。stats 已丢失但 track 仍在时，
直接丢弃该悬空 track 记录。

测试：`worker/cworker/reaper_recovery_test.go`
（`TestReaperTriageAfterRestart` 六态表、`TestReaperReEnqueueDeadPool` 校验
JobID/载荷保持、`TestReaperDoesNotTouchCurrentPool`、`TestReaperEmptyDeadPoolIsBestEffort`、
`TestReaperReEnqueueMultipleJobTypes`）；`runner/redis_recovery_test.go`
（`TestRedeliver*`）。

## 3. 重复回调为什么不会重复执行

幂等不是单点保证，而是四层防线：

1. **Redis 状态 CAS**（`SetStatusScript`）：仅允许 `revision` 前进，或同 revision
   下状态码单调前进；**相同状态重复提交被显式允许**（Lua 注释 "set same status
   repeatedly is allowed"），终态转换原子释放一半 track 计数。
   回退转换（final → running）被拒绝，持久化状态不会倒退。
2. **ACK CAS**（`HookAckScript`）：ack 水位（revision/status/check_in_at）只进不退；
   重复 ACK 幂等、不再递减计数；旧 revision 的迟到 ACK 返回 `none` 拒绝。
3. **hook agent 重试抑制**：后台重试每次发送前 `isOutdated` 比较 ack 水位，
   revision 落后、状态落后、check_in_at 落后的事件直接丢弃；首次投递失败不写 ack，
   重试与 reaper 会继续补发（指数退避，上限 5 分钟，之后 reaper 兜底）。
4. **core 数据库 CAS**：
   - `taskDAO.UpdateStatus` 单条 SQL 条件
     `status_revision = ? and status_code < ? or status_revision < ?`，
     重复回调影响 0 行；revision 变化（作业重试）才递增 run_count；
   - `executionDAO.RefreshStatus` 用 `where revision = ?` 乐观锁 + 最多 5 次重试；
   - HTTP 层任务 ID 与 JobID 绑定校验（`HandleForTask`），不匹配返回 400。

> 边界（已知行为）：
>
> - jobservice 边界提供的是 **at-least-once 通知**：终态同状态重放、以及被 CAS
>   拒绝的回退转换，都可能再发出一次状态事件——`tracker.compareAndSet` 用
>   `lib/errors.Wrap` 包装了 mismatch，`setStatus` 的直接类型断言因此失效，
>   被拒绝的转换仍会伴随一次陈旧 hook。Redis 状态不会错；接收端（第 4 层 CAS）
>   必须、也确实将其变为 0 行更新。
> - revision 为秒级精度：崩溃与重放发生在同一秒时 revision 可能相同，
>   重复抑制依赖状态码与 check_in_at，而非仅靠 revision。
> - 注册的 vendor 状态变更 post function 在每次回调时都会执行，重复回调下需自身幂等。

测试：`hook/hook_agent_recovery_test.go`（ACK 幂等、旧 ACK 拒绝、7 组水位矩阵、
失败投递无 ACK）；`runner/redis_recovery_test.go` 的
`TestStatusTransitionIdempotencyBoundary`、
`TestRedeliverCompletedJobDoesNotExecuteAgain`。

## 4. 取消（cancel）语义

- 运行中：`basicWorker.StopJob` → `tracker.Stop()`（CAS 到 Stopped）；作业实现通过
  轮询 `OPCommand()` 协作式退出；终态后重复 stop 直接返回 nil。
- 未运行的 Scheduled：额外调用 `DeleteScheduledJob` 从 zset 移除；不存在只告警
  （可能已在运行）。
- Pending 批量清理（jobmonitor）：`StopPendingJobs` 读出队列内全部 jobID 后 `DEL`
  就绪队列，并异步删除各 job stats / track；随后 `updateJobStatusInTask`
  将 task 批量置 Stopped，并对该 vendor 下所有 Pending execution 逐个
  `RefreshStatus` 收敛执行状态。
  - 批量 task 更新失败**不阻断** execution 刷新（两段互相 best-effort）；
  - 单个 execution 刷新失败不影响其余 execution；
  - worker 上报的运行中任务在取消前已完成（task 行查不到）时视为成功 no-op。

测试：`runner/redis_recovery_test.go`（`TestRedeliverStoppedJobStaysCancelled`）、
`job/impl/recovery_boundary_test.go`（`TestImplementationHonorsCooperativeStop`）、
`controller/jobmonitor/monitor_recovery_test.go`（`TestStop*`、`TestUpdateJobStatus*`、
`TestBatchTaskUpdateFailureDoesNotBlockExecutionRefresh`）。

## 5. 部分提交（partial commit）边界

- Redis 状态落库与 HTTP 回调投递**不是事务**：`setStatus` 先执行 Lua 提交，再 fire
  hook。若 Running hook 失败，作业会收敛为 `Error`（业务逻辑此时尚未开始）；
  track 计数停在 1，直到 Error 事件最终 ACK 才归零——reaper 因此会持续补发，
  不会静默丢失。
- core 侧 `stopJob` 使用本地事务（`orm.WithTransaction`）；
  `UpdateStatusInBatch` 使用单条原生 SQL 保证原子，但**跨 Redis（删队列）与
  DB（改 task/execution）没有分布式事务**：Redis 已删而 DB 未收敛的窗口由异步
  goroutine + execution 刷新补偿。
- lcm 的 `retryList`（状态写入失败的内存重试队列）**不持久化**，进程重启即丢失，
  由 reaper 周期对账兜底；因此极端窗口内回调可能延迟，但状态最终一致。
- unique 任务的去重标记在进程崩溃时不会被主动删除，相同 unique 任务在 24 小时 TTL
  内被拒绝（`ConflictError`），TTL 到期自愈。

测试：`TestHookFailureLeavesFailedFinalState`（含手工 ACK 闭环）、
`worker/cworker/de_duplicator_recovery_test.go`（`TestUniqueSignCrashLeaseBoundary`）。

## 6. 任务日志为何重启后仍可按 execution 查询

日志按 **JobID** 存放（FILE 或 DB backend，sweeper 按周期清理），与队列生命周期解耦；
查询链路为：

```
execution ID → task 表行（execution_id → job_id）→ jobservice /api/v1/jobs/<jobID>/log
```

死池回收只做 `RPOPLPUSH`，**JobID 不变**，因此同一执行的日志在重启/重放前后
都能通过相同的 job_id 取回；jobmonitor 的 `GetJobLog(jobID)` 直接委托
`task.Manager.GetLogByJobID`。作业实现重放产生的新日志追加在同一 JobID 下。

测试：`controller/jobmonitor/monitor_recovery_test.go` 的
`TestGetJobLogResolvesByJobID`；`runner/redis_recovery_test.go` 与
`job/impl/recovery_boundary_test.go` 中重放后断言 JobID 稳定。

## 7. 作业实现者必须遵守的契约（src/jobservice/job/impl）

1. 投递语义是 **at-least-once**：同一逻辑作业的 `Run` 可能执行多次，
   副作用必须幂等或可对账（不要把“只执行一次”的状态只放在进程内存）。
2. 重放保持相同 JobID 与参数；作业可通过 tracker/execution 侧标识做去重，
   不可自行生成每次执行变化的去重键。
3. 长任务必须轮询 `OPCommand()` 并在收到 stop 时尽快、干净地退出。
4. 终态（Success/Stopped/Error）之后框架不会再次调用 `Run`；
   但状态通知可能重复，vendor 注册的 post function 需幂等。
5. 进度上报（CheckIn）在重放时会被清空，属于新的一次尝试，不要把旧进度当作当前进度。

参考实现：`job/impl/recovery_boundary_test.go` 中的 `boundaryIdempotentJob` 与
`boundaryCancellableJob` 演示了上述两种必备模式。

## 8. 测试运行方式

新增测试除既有套件约定外，使用 `github.com/alicebob/miniredis/v2` 内嵌 Redis，
**无需外部 Redis / PostgreSQL**：

```bash
cd src
go test ./jobservice/worker/cworker/ -run 'TestReaper|TestUniqueSign'
go test ./jobservice/runner/        -run 'TestRedeliver|TestStatusTransition|TestHookFailure|TestReset'
go test ./jobservice/hook/          -run 'TestTriggerWritesAck|TestDuplicateAck|TestStaleAck|TestOutdatedCallback|TestFailedDelivery'
go test ./jobservice/job/impl/      -run 'TestImplementation'
go test ./controller/jobmonitor/    -run 'TestGetJobLog|TestStop|TestUpdateJobStatus|TestBatchTask|TestPendingExecution'
```

既有的 `*_test.go`（如 `c_worker_test.go`、`reaper_test.go`、`redis_test.go`）
仍按仓库约定依赖真实 Redis（`REDIS_HOST`）与 PostgreSQL（`POSTGRESQL_*`），
由 CI 的 docker 环境提供，与本次新增的自包含测试互补。
