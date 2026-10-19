// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/jobservice/common/rds"
	"github.com/goharbor/harbor/src/jobservice/common/utils"
	"github.com/goharbor/harbor/src/jobservice/env"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/jobservice/lcm"
	"github.com/goharbor/harbor/src/jobservice/logger"
)

// redisRunnerRecoveryHarness wires a miniredis-backed lcm controller and the
// redis job runner, no external redis/postgres required.
type redisRunnerRecoveryHarness struct {
	t      *testing.T
	ns     string
	pool   *redis.Pool
	lcmCtl lcm.Controller
	envCtx *env.Context
	hook   *recoveryHookRecorder
}

func newRedisRunnerRecoveryHarness(t *testing.T) *redisRunnerRecoveryHarness {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return redis.Dial("tcp", mr.Addr()) },
	}
	t.Cleanup(func() { _ = pool.Close() })

	hook := &recoveryHookRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	envCtx := &env.Context{
		SystemContext: ctx,
		WG:            new(sync.WaitGroup),
		ErrorChan:     make(chan error, 1),
		JobContext:    &recoveryJobContext{sysCtx: ctx},
	}
	ns := "recovery_runner_" + utils.MakeIdentifier()[:8]
	ctl := lcm.NewController(envCtx, ns, pool, hook.callback)

	return &redisRunnerRecoveryHarness{t: t, ns: ns, pool: pool, lcmCtl: ctl, envCtx: envCtx, hook: hook}
}

func (h *redisRunnerRecoveryHarness) saveJob(jid string, status string) job.Tracker {
	h.t.Helper()
	stats := &job.Stats{Info: &job.StatsInfo{
		JobID:       jid,
		JobName:     "RECOVERY_JOB",
		JobKind:     job.KindGeneric,
		Status:      status,
		WebHookURL:  "http://harbor-core/service/notifications/tasks/1",
		EnqueueTime: time.Now().Unix(),
	}}
	t, err := h.lcmCtl.New(stats)
	require.NoError(h.t, err)
	// Save() persists the generated revision but does not write it back to the
	// in-memory stats; reload so direct status transitions pass the right CAS revision.
	t, err = h.lcmCtl.Track(jid)
	require.NoError(h.t, err)
	return t
}

func (h *redisRunnerRecoveryHarness) run(jid string, args map[string]any) error {
	return NewRedisJob((*recoveryCountingJob)(nil), h.envCtx, h.lcmCtl).Run(&work.Job{ID: jid, Name: "RECOVERY_JOB", Args: args})
}

func (h *redisRunnerRecoveryHarness) status(jid string) job.Status {
	h.t.Helper()
	t, err := h.lcmCtl.Track(jid)
	require.NoError(h.t, err)
	return job.Status(t.Job().Info.Status)
}

// recoveryHookRecorder records fired status change events and can be switched
// to failure mode to emulate a lost core callback.
type recoveryHookRecorder struct {
	mu      sync.Mutex
	events  []*job.StatusChange
	failFor map[string]bool
}

func (r *recoveryHookRecorder) callback(_ string, change *job.StatusChange) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, change)
	if r.failFor[change.Status] {
		return context.DeadlineExceeded
	}
	return nil
}

func (r *recoveryHookRecorder) count(status string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Status == status {
			n++
		}
	}
	return n
}

// recoveryJobContext is a minimal job.Context without the beego orm / DB
// logger stack, enough for RedisJob.Run to build an execution context.
type recoveryJobContext struct {
	sysCtx  context.Context
	tracker job.Tracker
}

func (c *recoveryJobContext) Build(t job.Tracker) (job.Context, error) {
	return &recoveryJobContext{sysCtx: c.sysCtx, tracker: t}, nil
}

func (c *recoveryJobContext) Get(string) (any, bool) { return nil, false }

func (c *recoveryJobContext) SystemContext() context.Context { return c.sysCtx }

func (c *recoveryJobContext) Checkin(message string) error { return c.tracker.CheckIn(message) }

func (c *recoveryJobContext) OPCommand() (job.OPCommand, bool) {
	st, err := c.tracker.Status()
	if err != nil {
		return job.NilCommand, false
	}
	if st == job.StoppedStatus {
		return job.StopCommand, true
	}
	return job.NilCommand, false
}

func (c *recoveryJobContext) GetLogger() logger.Interface { return recoveryLogger{} }

func (c *recoveryJobContext) Tracker() job.Tracker { return c.tracker }

type recoveryLogger struct{}

func (recoveryLogger) Debug(...any)            {}
func (recoveryLogger) Debugf(string, ...any)   {}
func (recoveryLogger) Info(...any)             {}
func (recoveryLogger) Infof(string, ...any)    {}
func (recoveryLogger) Warning(...any)          {}
func (recoveryLogger) Warningf(string, ...any) {}
func (recoveryLogger) Error(...any)            {}
func (recoveryLogger) Errorf(string, ...any)   {}
func (recoveryLogger) Fatal(...any)            {}
func (recoveryLogger) Fatalf(string, ...any)   {}
func (recoveryLogger) Close() error            { return nil }

// recoveryCountingJob reports business-level executions through globals:
// runner.Wrap builds a fresh instance per dispatch, so shared counters cannot
// live on the struct. It also remembers the arguments of the last execution to
// prove replay keeps the original job parameters.
var (
	recoveryJobRunCount atomic.Int64
	recoveryJobArgs     atomic.Value
)

type recoveryCountingJob struct{}

func (j *recoveryCountingJob) MaxFails() uint                { return 1 }
func (j *recoveryCountingJob) MaxCurrency() uint             { return 0 }
func (j *recoveryCountingJob) ShouldRetry() bool             { return false }
func (j *recoveryCountingJob) Validate(job.Parameters) error { return nil }

func (j *recoveryCountingJob) Run(_ job.Context, params job.Parameters) error {
	recoveryJobRunCount.Add(1)
	recoveryJobArgs.Store(params)
	return nil
}

// TestRedeliverClaimedRunningJobAfterRestart is the core failure-recovery
// contract: a job already claimed (status Running) when the process died is
// reset to Pending with a bumped revision and executed exactly once on
// redelivery. Its log remains reachable because the work.Job identity never
// changes.
func TestRedeliverClaimedRunningJobAfterRestart(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "claimed-" + utils.MakeIdentifier()[:10]

	// First launch: Pending -> Running, then the worker crashes mid-flight.
	tracker := h.saveJob(jid, job.PendingStatus.String())
	require.NoError(t, tracker.Run())
	// Revision is a unix-second timestamp; age it so the reset bump is
	// observable even when the test runs well within one second.
	oldRev := time.Now().Add(-10 * time.Second).Unix()
	conn := h.pool.Get()
	_, err := conn.Do("HSET", rds.KeyJobStats(h.ns, jid), "revision", oldRev)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	beforeRuns := recoveryJobRunCount.Load()

	// Restart: gocraft/work re-enqueued the in-progress payload and the runner
	// receives the SAME job ID.
	require.NoError(t, h.run(jid, map[string]any{"image": "demo:replay"}))

	require.Equal(t, job.SuccessStatus, h.status(jid), "replayed job must finish successfully")
	require.Equal(t, beforeRuns+1, recoveryJobRunCount.Load(), "business Run must execute exactly once on replay")
	require.Equal(t, job.Parameters{"image": "demo:replay"}, recoveryJobArgs.Load().(job.Parameters),
		"replay must deliver the original parameters")

	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	require.Greater(t, reloaded.Job().Info.Revision, oldRev,
		"reset must bump the revision so stale callbacks are rejected")
	// Status sequence observed by core: pre-crash Running, replay Running, Success.
	require.Equal(t, 2, h.hook.count(job.RunningStatus.String()))
	require.Equal(t, 1, h.hook.count(job.SuccessStatus.String()))
}

// TestRedeliverCompletedJobDoesNotExecuteAgain pins the other half of the
// contract: a job already in a final state must never run its business logic
// again. Note the boundary — the Success status hook may still be re-fired;
// receivers (core task DAO) make that idempotent with a revision/status CAS.
func TestRedeliverCompletedJobDoesNotExecuteAgain(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "done-" + utils.MakeIdentifier()[:10]

	h.saveJob(jid, job.PendingStatus.String())
	require.NoError(t, h.run(jid, nil), "first execution")
	require.Equal(t, job.SuccessStatus, h.status(jid))
	runsAfterFirst := recoveryJobRunCount.Load()
	successHooks := h.hook.count(job.SuccessStatus.String())

	// Duplicate delivery of the already-completed job (e.g. double requeue).
	require.NoError(t, h.run(jid, nil))

	require.Equal(t, runsAfterFirst, recoveryJobRunCount.Load(),
		"business logic must NOT run twice for a completed job")
	require.Equal(t, job.SuccessStatus, h.status(jid))
	require.Equal(t, successHooks+1, h.hook.count(job.SuccessStatus.String()),
		"boundary: terminal status notification may be re-sent; receiver-side CAS keeps it idempotent")
}

// TestRedeliverStoppedJobStaysCancelled verifies cancellation observed while
// the worker was dead is honored: no execution, no status regress, no hook.
func TestRedeliverStoppedJobStaysCancelled(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "cancelled-" + utils.MakeIdentifier()[:10]

	tracker := h.saveJob(jid, job.RunningStatus.String())
	beforeRuns := recoveryJobRunCount.Load()
	require.NoError(t, tracker.Stop(), "cancel issued while the worker pool was down")
	stopHooks := h.hook.count(job.StoppedStatus.String())

	require.NoError(t, h.run(jid, nil), "redelivery after cancel")

	require.Equal(t, beforeRuns, recoveryJobRunCount.Load(), "cancelled job must never execute")
	require.Equal(t, job.StoppedStatus, h.status(jid))
	require.Equal(t, stopHooks, h.hook.count(job.StoppedStatus.String()),
		"stopped redelivery must not re-fire any hook")
}

// TestRedeliverErrorJobResetsAndRetries verifies a job that died in Error is
// reset and retried on redelivery, and can converge to Success.
func TestRedeliverErrorJobResetsAndRetries(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "error-" + utils.MakeIdentifier()[:10]

	h.saveJob(jid, job.ErrorStatus.String())
	oldRev := time.Now().Add(-10 * time.Second).Unix()
	conn := h.pool.Get()
	_, err := conn.Do("HSET", rds.KeyJobStats(h.ns, jid), "revision", oldRev)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	beforeRuns := recoveryJobRunCount.Load()

	require.NoError(t, h.run(jid, nil))

	require.Equal(t, job.SuccessStatus, h.status(jid))
	require.Equal(t, beforeRuns+1, recoveryJobRunCount.Load())
	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	require.Greater(t, reloaded.Job().Info.Revision, oldRev)
}

// TestStatusTransitionIdempotencyBoundary documents where duplicate-callback
// protection actually lives:
//   - a regressive transition (final -> running) is rejected by the CAS script,
//     so the persisted status never regresses;
//   - repeating the SAME final status is allowed by redis and RE-EMITS the
//     hook ("set same status repeatedly is allowed" in the Lua);
//   - known quirk: compareAndSet wraps the mismatch error, so tracker.setStatus
//     still emits one stale notification for the rejected transition.
//
// Exactly-once is therefore not provided at the jobservice boundary: the hook
// agent ACK and the core task-DAO (status_revision, status_code) CAS are what
// turn every such repeated/stale notification into a no-op.
func TestStatusTransitionIdempotencyBoundary(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "terminal-" + utils.MakeIdentifier()[:10]
	tracker := h.saveJob(jid, job.PendingStatus.String())

	require.NoError(t, tracker.Run())
	require.NoError(t, tracker.Succeed())
	require.Equal(t, 1, h.hook.count(job.SuccessStatus.String()))

	// Regressive transition is rejected in redis.
	err := tracker.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch job status", "final -> running must mismatch: %v", err)
	require.Equal(t, job.SuccessStatus, h.status(jid), "persisted status must never regress")
	// Known quirk: the rejected transition still emits a stale Running hook once;
	// the receiver task-DAO CAS (same revision, higher current status code)
	// updates zero rows.
	require.Equal(t, 1, h.hook.count(job.RunningStatus.String()),
		"known boundary: a stale hook accompanies the rejected transition, receiver CAS drops it")

	// Same-status repeat is accepted and re-emits the hook; receivers must dedup.
	require.NoError(t, tracker.Succeed(), "same-status repeat is intentionally allowed")
	require.Equal(t, 2, h.hook.count(job.SuccessStatus.String()),
		"boundary: redis re-emits repeated same-status hooks; ACK/DAO CAS must suppress duplicates")
}

// TestHookFailureLeavesFailedFinalState documents the partial-commit boundary:
// the Redis status transition commits independently of webhook delivery. When
// the callback fails, Running is committed first, then the run folds to Error —
// business logic never started, and the reaper/hook-agent reconcile delivery
// afterwards.
func TestHookFailureLeavesFailedFinalState(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	h.hook.failFor = map[string]bool{
		job.RunningStatus.String(): true,
		job.ErrorStatus.String():   true,
	}
	jid := "hookfail-" + utils.MakeIdentifier()[:10]
	h.saveJob(jid, job.PendingStatus.String())
	beforeRuns := recoveryJobRunCount.Load()

	err := h.run(jid, nil)
	require.Error(t, err, "a failed status hook surfaces as a run error")

	require.Equal(t, job.ErrorStatus, h.status(jid), "job must fold to Error, never stay hung in Running")
	require.Equal(t, beforeRuns, recoveryJobRunCount.Load(), "business logic must not have started")
	require.GreaterOrEqual(t, h.hook.count(job.RunningStatus.String()), 1,
		"partial commit: the Running event was attempted before the failure")

	// Partial-commit boundary: the in-progress counter starts at 2; one half is
	// released by the final status transition and the other half is released
	// only when the status hook is ACKed. Because delivery failed, the counter
	// intentionally stays at 1 so the reaper keeps re-firing the Error event.
	conn := h.pool.Get()
	defer func() { _ = conn.Close() }()
	tracked, err := redis.Int(conn.Do("HGET", rds.KeyJobTrackInProgress(h.ns), jid))
	require.NoError(t, err)
	require.Equal(t, 1, tracked, "missing ACK must keep the job tracked for reaper-driven re-delivery")

	// Once the hook agent finally delivers and stores the ACK (HookAckScript),
	// the second half is released and the in-progress record is dropped.
	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	reply, err := redis.String(rds.HookAckScript.Do(
		conn,
		rds.KeyJobStats(h.ns, jid),
		rds.KeyJobTrackInProgress(h.ns),
		job.ErrorStatus.String(),
		reloaded.Job().Info.Revision,
		int64(0),
		jid,
	))
	require.NoError(t, err)
	require.Equal(t, "ok", reply)
	_, err = redis.Int(conn.Do("HGET", rds.KeyJobTrackInProgress(h.ns), jid))
	require.Equal(t, redis.ErrNil, err, "status + ACK together must release the track record")
}

// TestResetClearsStaleCheckIn verifies replay wipes check-in progress of the
// interrupted attempt: the replayed execution starts from a clean progress
// state instead of resurrecting pre-crash progress.
func TestResetClearsStaleCheckIn(t *testing.T) {
	h := newRedisRunnerRecoveryHarness(t)
	jid := "checkin-" + utils.MakeIdentifier()[:10]
	tracker := h.saveJob(jid, job.RunningStatus.String())
	require.NoError(t, tracker.CheckIn("progress: 60%"))
	require.Positive(t, tracker.Job().Info.CheckInAt)

	require.NoError(t, h.run(jid, nil))

	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	require.Equal(t, job.SuccessStatus, job.Status(reloaded.Job().Info.Status))
}

var (
	_ job.Context      = (*recoveryJobContext)(nil)
	_ logger.Interface = recoveryLogger{}
	_ logger.Closer    = recoveryLogger{}
	_ job.Interface    = (*recoveryCountingJob)(nil)
)
