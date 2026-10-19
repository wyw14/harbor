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

// Package impl_test pins down the contract a job implementation must honor
// under the persistent queue's failure model. The queue provides AT LEAST
// ONCE delivery, not exactly once: after a worker crash a claimed (Running or
// Error) job is re-enqueued with the same JobID/parameters and Run again.
// Implementations therefore must:
//
//  1. tolerate Run being invoked more than once for the same logical job
//     (make side effects idempotent or reconcile-able);
//  2. assume the process may die at any point and cooperate with cancellation
//     by polling OPCommand;
//  3. never rely on "Run was called once" state held only in memory.
package impl_test

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
	"github.com/goharbor/harbor/src/jobservice/runner"
)

// ---- minimal job.Context (no beego orm / file loggers needed) ----

type boundaryContext struct {
	ctx     context.Context
	tracker job.Tracker
}

func (c *boundaryContext) Build(t job.Tracker) (job.Context, error) {
	return &boundaryContext{ctx: c.ctx, tracker: t}, nil
}
func (c *boundaryContext) Get(string) (any, bool)         { return nil, false }
func (c *boundaryContext) SystemContext() context.Context { return c.ctx }
func (c *boundaryContext) Checkin(message string) error   { return c.tracker.CheckIn(message) }
func (c *boundaryContext) GetLogger() logger.Interface    { return boundaryLogger{} }
func (c *boundaryContext) Tracker() job.Tracker           { return c.tracker }
func (c *boundaryContext) OPCommand() (job.OPCommand, bool) {
	st, err := c.tracker.Status()
	if err != nil {
		return job.NilCommand, false
	}
	if st == job.StoppedStatus {
		return job.StopCommand, true
	}
	return job.NilCommand, false
}

type boundaryLogger struct{}

func (boundaryLogger) Debug(...any)            {}
func (boundaryLogger) Debugf(string, ...any)   {}
func (boundaryLogger) Info(...any)             {}
func (boundaryLogger) Infof(string, ...any)    {}
func (boundaryLogger) Warning(...any)          {}
func (boundaryLogger) Warningf(string, ...any) {}
func (boundaryLogger) Error(...any)            {}
func (boundaryLogger) Errorf(string, ...any)   {}
func (boundaryLogger) Fatal(...any)            {}
func (boundaryLogger) Fatalf(string, ...any)   {}
func (boundaryLogger) Close() error            { return nil }

// ---- harness ----

type boundaryHarness struct {
	t      *testing.T
	ns     string
	pool   *redis.Pool
	lcmCtl lcm.Controller
	envCtx *env.Context
}

func newBoundaryHarness(t *testing.T) *boundaryHarness {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return redis.Dial("tcp", mr.Addr()) },
	}
	t.Cleanup(func() { _ = pool.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	envCtx := &env.Context{
		SystemContext: ctx,
		WG:            new(sync.WaitGroup),
		ErrorChan:     make(chan error, 1),
		JobContext:    &boundaryContext{ctx: ctx},
	}
	ns := "boundary_impl_" + utils.MakeIdentifier()[:8]
	ctl := lcm.NewController(envCtx, ns, pool, func(string, *job.StatusChange) error { return nil })
	return &boundaryHarness{t: t, ns: ns, pool: pool, lcmCtl: ctl, envCtx: envCtx}
}

func (h *boundaryHarness) save(jid, status string) job.Tracker {
	h.t.Helper()
	t, err := h.lcmCtl.New(&job.Stats{Info: &job.StatsInfo{
		JobID:       jid,
		JobName:     "BOUNDARY_JOB",
		JobKind:     job.KindGeneric,
		Status:      status,
		EnqueueTime: time.Now().Unix(),
	}})
	require.NoError(h.t, err)
	t, err = h.lcmCtl.Track(jid)
	require.NoError(h.t, err)
	return t
}

func (h *boundaryHarness) dispatch(j any, jid string, args map[string]any) error {
	return runner.NewRedisJob(j, h.envCtx, h.lcmCtl).Run(&work.Job{ID: jid, Name: "BOUNDARY_JOB", Args: args})
}

// ---- reference job implementations ----

var boundaryRunCount atomic.Int64

// boundaryIdempotentJob models a well-behaved implementation: it records the
// delivery (so tests can observe replays) and treats repeated Run as normal.
type boundaryIdempotentJob struct{}

func (boundaryIdempotentJob) MaxFails() uint                { return 1 }
func (boundaryIdempotentJob) MaxCurrency() uint             { return 0 }
func (boundaryIdempotentJob) ShouldRetry() bool             { return false }
func (boundaryIdempotentJob) Validate(job.Parameters) error { return nil }
func (boundaryIdempotentJob) Run(_ job.Context, _ job.Parameters) error {
	boundaryRunCount.Add(1)
	return nil
}

// boundaryCancellableJob models the required cooperative-cancel pattern: side
// effects happen only if no Stop command is observed.
type boundaryCancellableJob struct {
	sideEffect *atomic.Bool
}

func (boundaryCancellableJob) MaxFails() uint                { return 1 }
func (boundaryCancellableJob) MaxCurrency() uint             { return 0 }
func (boundaryCancellableJob) ShouldRetry() bool             { return false }
func (boundaryCancellableJob) Validate(job.Parameters) error { return nil }
func (j boundaryCancellableJob) Run(ctx job.Context, _ job.Parameters) error {
	if cmd, ok := ctx.OPCommand(); ok && cmd == job.StopCommand {
		return nil // leave sideEffect untouched
	}
	j.sideEffect.Store(true)
	return nil
}

// TestImplementationReceivesSameIdentityOnReplay verifies the core at-least-
// once fact: a claimed job re-delivered after a crash enters Run again with
// the same JobID/parameters. Implementations MUST be idempotent.
func TestImplementationReceivesSameIdentityOnReplay(t *testing.T) {
	h := newBoundaryHarness(t)
	jid := "boundary-replay-" + utils.MakeIdentifier()[:10]

	tracker := h.save(jid, job.PendingStatus.String())
	require.NoError(t, tracker.Run()) // claimed by the worker that then crashes
	// Age the revision so Reset() deterministically bumps it.
	conn := h.pool.Get()
	_, err := conn.Do("HSET", rds.KeyJobStats(h.ns, jid), "revision", time.Now().Add(-10*time.Second).Unix())
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	before := boundaryRunCount.Load()
	require.NoError(t, h.dispatch(boundaryIdempotentJob{}, jid, map[string]any{"artifact": "demo:v1"}))
	require.Equal(t, before+1, boundaryRunCount.Load(),
		"contract: Run MUST be expected to execute again after a crash; make side effects idempotent")

	// The replayed execution is queryable under the exact same identity.
	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	require.Equal(t, job.SuccessStatus.String(), reloaded.Job().Info.Status)
	require.Equal(t, jid, reloaded.Job().Info.JobID, "JobID is stable across replay; logs keyed by it stay reachable")
}

// TestImplementationNotReenteredAfterSuccess verifies once a job reached a
// final state, a stray duplicate dispatch never reaches Run again — business
// logic duplication is prevented even though status hooks may still repeat.
func TestImplementationNotReenteredAfterSuccess(t *testing.T) {
	h := newBoundaryHarness(t)
	jid := "boundary-done-" + utils.MakeIdentifier()[:10]

	h.save(jid, job.PendingStatus.String())
	require.NoError(t, h.dispatch(boundaryIdempotentJob{}, jid, nil))
	afterFirst := boundaryRunCount.Load()

	require.NoError(t, h.dispatch(boundaryIdempotentJob{}, jid, nil))
	require.Equal(t, afterFirst, boundaryRunCount.Load(),
		"contract: a completed job's Run must never execute again")
}

// TestImplementationHonorsCooperativeStop verifies the cancellation pattern
// implementations must use: poll OPCommand and skip side effects when stopped.
func TestImplementationHonorsCooperativeStop(t *testing.T) {
	h := newBoundaryHarness(t)
	jid := "boundary-stop-" + utils.MakeIdentifier()[:10]

	tracker := h.save(jid, job.PendingStatus.String())
	require.NoError(t, tracker.Run())
	require.NoError(t, tracker.Stop()) // cancel while claimed

	var sideEffect atomic.Bool
	require.NoError(t, h.dispatch(boundaryCancellableJob{sideEffect: &sideEffect}, jid, nil))
	require.False(t, sideEffect.Load(),
		"contract: after cancellation Run must observe Stop and avoid further side effects")

	reloaded, err := h.lcmCtl.Track(jid)
	require.NoError(t, err)
	require.Equal(t, job.StoppedStatus.String(), reloaded.Job().Info.Status)
}

var (
	_ job.Context   = (*boundaryContext)(nil)
	_ job.Interface = boundaryIdempotentJob{}
)
