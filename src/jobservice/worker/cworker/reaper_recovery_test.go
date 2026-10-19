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

package cworker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/jobservice/common/rds"
	"github.com/goharbor/harbor/src/jobservice/common/utils"
	"github.com/goharbor/harbor/src/jobservice/env"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/jobservice/lcm"
)

// fakeJobType is only used as a second known-job key in the redis structures.
const fakeJobType = "FAKE_RECOVERY_JOB"

// recoveryWebhook is registered on every seeded stats hash; the tracker only
// invokes the lcm callback when a hook URL is present.
const recoveryWebhook = "http://harbor-core/service/notifications/tasks/1"

// recordedHook keeps every status change event the lcm controller fires, so the
// reaper triage decisions (re-fire / stay silent) become observable.
type recordedHook struct {
	mu     sync.Mutex
	events []*job.StatusChange
	err    error
}

func (h *recordedHook) callback(_ string, change *job.StatusChange) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, change)
	return h.err
}

func (h *recordedHook) statuses() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) == 0 {
		return nil
	}
	out := make([]string, 0, len(h.events))
	for _, e := range h.events {
		out = append(out, e.Status)
	}
	return out
}

func newMiniRedisPool(t *testing.T) (*miniredis.Miniredis, *redis.Pool) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return redis.Dial("tcp", mr.Addr()) },
	}
	t.Cleanup(func() { _ = pool.Close() })
	return mr, pool
}

func newRecoveryReaper(t *testing.T, ns string, pool *redis.Pool, ctl lcm.Controller, jobTypes ...string) *reaper {
	t.Helper()
	return &reaper{
		context:   context.Background(),
		namespace: ns,
		pool:      pool,
		lcmCtl:    ctl,
		jobTypes:  jobTypes,
	}
}

// seedJobStats writes job stats directly, exactly like the gocraft/work worker
// would have left them in redis when the process died at a given moment.
func seedJobStats(t *testing.T, pool *redis.Pool, ns, jid, jname, status string, revision int64) {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	args := []any{
		rds.KeyJobStats(ns, jid),
		"id", jid,
		"name", jname,
		"kind", job.KindGeneric,
		"unique", 0,
		"status", status,
		"ref_link", "/api/v1/jobs/" + jid,
		"web_hook_url", recoveryWebhook,
		"enqueue_time", time.Now().Unix(),
		"update_time", time.Now().Unix(),
		"revision", revision,
	}
	_, err := conn.Do("HMSET", args...)
	require.NoError(t, err)
}

func seedAck(t *testing.T, pool *redis.Pool, ns, jid, status string, revision, checkInAt int64) {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	b, err := json.Marshal(&job.ACK{Status: status, Revision: revision, CheckInAt: checkInAt})
	require.NoError(t, err)
	_, err = conn.Do("HSET", rds.KeyJobStats(ns, jid), "ack", string(b))
	require.NoError(t, err)
}

func trackInProgress(t *testing.T, pool *redis.Pool, ns, jid string, count int64) {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	_, err := conn.Do("HSET", rds.KeyJobTrackInProgress(ns), jid, count)
	require.NoError(t, err)
}

func statusOf(t *testing.T, pool *redis.Pool, ns, jid string) string {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	v, err := redis.String(conn.Do("HGET", rds.KeyJobStats(ns, jid), "status"))
	require.NoError(t, err)
	return v
}

func inProgressTracked(t *testing.T, pool *redis.Pool, ns, jid string) bool {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	n, err := redis.Int(conn.Do("HGET", rds.KeyJobTrackInProgress(ns), jid))
	if err == redis.ErrNil {
		return false
	}
	require.NoError(t, err)
	return n > 0
}

// TestReaperTriageAfterRestart pins down what the reaper does with each kind of
// record left behind by a crashed jobservice: claimed jobs still running,
// finished jobs whose callback was lost, expired/hung jobs and lost stats.
func TestReaperTriageAfterRestart(t *testing.T) {
	cases := []struct {
		name string
		// mutate arranges the redis state before the reaper sweep.
		mutate func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64)
		// wantStatus is the expected persisted status after the sweep.
		wantStatus string
		// wantHook is the status sequence the reaper re-fired (empty = none).
		wantHook []string
		// wantTracked asserts the in-progress track record presence afterwards.
		wantTracked bool
	}{
		{
			name: "claimed running, hook never ACKed, fresh update time -> re-fire running hook and keep claimed",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64) {
				seedJobStats(t, pool, ns, jid, job.SampleJob, job.RunningStatus.String(), rev)
				trackInProgress(t, pool, ns, jid, 1)
			},
			wantStatus:  job.RunningStatus.String(),
			wantHook:    []string{job.RunningStatus.String()},
			wantTracked: true,
		},
		{
			name: "completed but final callback lost -> re-fire final hook and keep success",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64) {
				seedJobStats(t, pool, ns, jid, job.SampleJob, job.SuccessStatus.String(), rev)
				trackInProgress(t, pool, ns, jid, 1)
			},
			wantStatus:  job.SuccessStatus.String(),
			wantHook:    []string{job.SuccessStatus.String()},
			wantTracked: true,
		},
		{
			name: "completed and callback consistent -> untrack, no duplicate callback",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64) {
				seedJobStats(t, pool, ns, jid, job.SampleJob, job.SuccessStatus.String(), rev)
				seedAck(t, pool, ns, jid, job.SuccessStatus.String(), rev, 0)
				trackInProgress(t, pool, ns, jid, 1)
			},
			wantStatus:  job.SuccessStatus.String(),
			wantHook:    nil,
			wantTracked: false,
		},
		{
			name: "claimed job hung over max update duration -> marked error and error hook fired",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64) {
				seedJobStats(t, pool, ns, jid, job.SampleJob, job.RunningStatus.String(), rev)
				// Consistent Running ACK makes diff==0: the reaper then inspects
				// update_time instead of simply resending the hook.
				seedAck(t, pool, ns, jid, job.RunningStatus.String(), rev, 0)
				trackInProgress(t, pool, ns, jid, 1)
				// Roll update_time 25 hours back, beyond the 24h hung threshold.
				conn := pool.Get()
				defer func() { _ = conn.Close() }()
				old := time.Now().Add(-25 * time.Hour).Unix()
				_, err := conn.Do("HSET", rds.KeyJobStats(ns, jid), "update_time", old)
				require.NoError(t, err)
			},
			wantStatus:  job.ErrorStatus.String(),
			wantHook:    []string{job.ErrorStatus.String()},
			wantTracked: false,
		},
		{
			name: "stored status behind ACKed status -> adopt ACK status without re-firing hook",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, rev int64) {
				seedJobStats(t, pool, ns, jid, job.SampleJob, job.RunningStatus.String(), rev)
				seedAck(t, pool, ns, jid, job.SuccessStatus.String(), rev, 0)
				trackInProgress(t, pool, ns, jid, 1)
			},
			// Adopting the final ACK status runs the same CAS script as a normal
			// transition, so the in-progress counter is also released here.
			wantStatus:  job.SuccessStatus.String(),
			wantHook:    nil,
			wantTracked: false,
		},
		{
			name: "job stats lost while tracked -> discard the dangling track record",
			mutate: func(t *testing.T, pool *redis.Pool, ns, jid string, _ int64) {
				conn := pool.Get()
				defer func() { _ = conn.Close() }()
				_, err := conn.Do("HSET", rds.KeyJobTrackInProgress(ns), jid, 1)
				require.NoError(t, err)
			},
			wantStatus:  "",
			wantHook:    nil,
			wantTracked: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pool := newMiniRedisPool(t)
			ns := "recovery_reaper_" + utils.MakeIdentifier()[:8]

			hook := &recordedHook{}
			envCtx := &env.Context{SystemContext: context.Background()}
			ctl := lcm.NewController(envCtx, ns, pool, hook.callback)

			jid := "job-" + utils.MakeIdentifier()[:12]
			rev := time.Now().Unix()
			tc.mutate(t, pool, ns, jid, rev)

			r := newRecoveryReaper(t, ns, pool, ctl, job.SampleJob)
			require.NoError(t, r.syncOutdatedStats())

			require.Equal(t, tc.wantHook, hook.statuses(), "re-fired hook sequence mismatch")
			require.Equal(t, tc.wantTracked, inProgressTracked(t, pool, ns, jid), "in-progress track state mismatch")
			if tc.wantStatus != "" {
				require.Equal(t, tc.wantStatus, statusOf(t, pool, ns, jid))
			}
		})
	}
}

// TestReaperReEnqueueDeadPool verifies the restart one-shot recovery: jobs that
// a dead worker pool had BRPOPLPUSHed (claimed) are atomically moved back to
// the ready queue, the concurrency locks are released and the ORIGINAL job
// payload/ID is preserved, so a redelivered job keeps the same identity (and
// its log keyed by that ID stays reachable).
func TestReaperReEnqueueDeadPool(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_requeue_" + utils.MakeIdentifier()[:8]
	hook := &recordedHook{}
	ctl := lcm.NewController(&env.Context{SystemContext: context.Background()}, ns, pool, hook.callback)
	r := newRecoveryReaper(t, ns, pool, ctl, job.SampleJob)

	const (
		deadPool = "dead-pool-1"
		livePool = "restarted-pool-1"
	)
	conn := pool.Get()
	defer func() { _ = conn.Close() }()

	// Only the restarted pool registers a heartbeat; the dead pool is gone.
	_, err := conn.Do("SADD", rds.KeyWorkerPools(ns), livePool)
	require.NoError(t, err)

	// Two claimed jobs sitting in the dead pool in-progress list.
	wantIDs := []string{"job-claim-1", "job-claim-2"}
	for _, id := range wantIDs {
		payload, err := json.Marshal(map[string]any{
			"name": job.SampleJob,
			"id":   id,
			"t":    time.Now().Unix(),
			"args": map[string]any{"image": "demo:v1"},
		})
		require.NoError(t, err)
		_, err = conn.Do("LPUSH", rds.KeyInProgressQueue(ns, job.SampleJob, deadPool), string(payload))
		require.NoError(t, err)
	}
	// gocraft/work lock accounting: one counter per claimed job.
	_, err = conn.Do("SET", rds.KeyJobLock(ns, job.SampleJob), len(wantIDs))
	require.NoError(t, err)
	_, err = conn.Do("HSET", rds.KeyJobLockInfo(ns, job.SampleJob), deadPool, len(wantIDs))
	require.NoError(t, err)

	require.NoError(t, r.reEnqueueInProgressJobs())

	ready, err := redis.Strings(conn.Do("LRANGE", rds.KeyJobs(ns, job.SampleJob), 0, -1))
	require.NoError(t, err)
	require.Len(t, ready, len(wantIDs), "both claimed jobs must be back in the ready queue")

	gotIDs := make(map[string]bool, len(wantIDs))
	for _, raw := range ready {
		var j struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal([]byte(raw), &j))
		gotIDs[j.ID] = true
	}
	for _, id := range wantIDs {
		require.True(t, gotIDs[id], "redelivered job must keep its original ID %s", id)
	}

	inProgress, err := redis.Int(conn.Do("LLEN", rds.KeyInProgressQueue(ns, job.SampleJob, deadPool)))
	require.NoError(t, err)
	require.Zero(t, inProgress, "dead pool in-progress queue must be drained")

	lock, err := redis.Int(conn.Do("GET", rds.KeyJobLock(ns, job.SampleJob)))
	require.NoError(t, err)
	require.Zero(t, lock, "concurrency lock must be released for requeued jobs")
}

// TestReaperDoesNotTouchCurrentPool verifies a live pool's claimed jobs are not
// double-dispatched: the upstream gocraft/work reaper owns those, and moving
// them again would execute in-flight work twice.
func TestReaperDoesNotTouchCurrentPool(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_live_" + utils.MakeIdentifier()[:8]
	ctl := lcm.NewController(&env.Context{SystemContext: context.Background()}, ns, pool,
		(&recordedHook{}).callback)
	r := newRecoveryReaper(t, ns, pool, ctl, job.SampleJob)

	const livePool = "live-pool-1"
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	_, err := conn.Do("SADD", rds.KeyWorkerPools(ns), livePool)
	require.NoError(t, err)
	payload := `{"name":"DEMO","id":"job-live-1","t":1,"args":{}}`
	_, err = conn.Do("LPUSH", rds.KeyInProgressQueue(ns, job.SampleJob, livePool), payload)
	require.NoError(t, err)
	_, err = conn.Do("SET", rds.KeyJobLock(ns, job.SampleJob), 1)
	require.NoError(t, err)
	_, err = conn.Do("HSET", rds.KeyJobLockInfo(ns, job.SampleJob), livePool, 1)
	require.NoError(t, err)

	require.NoError(t, r.reEnqueueInProgressJobs())

	inProgress, err := redis.Int(conn.Do("LLEN", rds.KeyInProgressQueue(ns, job.SampleJob, livePool)))
	require.NoError(t, err)
	require.Equal(t, 1, inProgress, "claimed job of a live pool must stay in its in-progress queue")

	ready, err := redis.Int(conn.Do("LLEN", rds.KeyJobs(ns, job.SampleJob)))
	require.NoError(t, err)
	require.Zero(t, ready, "no job may be moved back to the ready queue")
}

// TestReaperEmptyDeadPoolIsBestEffort documents the boundary of the one-shot
// sweep: a stale lock_info entry without any in-progress payload is left
// untouched (only successfully moved jobs release locks). It must not surface
// an error that would abort startup.
func TestReaperEmptyDeadPoolIsBestEffort(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_empty_" + utils.MakeIdentifier()[:8]
	ctl := lcm.NewController(&env.Context{SystemContext: context.Background()}, ns, pool,
		(&recordedHook{}).callback)
	r := newRecoveryReaper(t, ns, pool, ctl, job.SampleJob)

	const deadPool = "dead-empty-pool"
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	_, err := conn.Do("SET", rds.KeyJobLock(ns, job.SampleJob), 1)
	require.NoError(t, err)
	_, err = conn.Do("HSET", rds.KeyJobLockInfo(ns, job.SampleJob), deadPool, 1)
	require.NoError(t, err)
	// No payload in the in-progress queue.

	require.NoError(t, r.reEnqueueInProgressJobs(), "empty dead pool must not fail startup")

	lock, err := redis.Int(conn.Do("GET", rds.KeyJobLock(ns, job.SampleJob)))
	require.NoError(t, err)
	require.Equal(t, 1, lock, "boundary: stale counters are not reconciled without a payload")
}

// TestReaperReEnqueueMultipleJobTypes verifies recovery iterates every
// registered job type, not just the first queue.
func TestReaperReEnqueueMultipleJobTypes(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_multi_" + utils.MakeIdentifier()[:8]
	ctl := lcm.NewController(&env.Context{SystemContext: context.Background()}, ns, pool,
		(&recordedHook{}).callback)
	r := newRecoveryReaper(t, ns, pool, ctl, job.SampleJob, fakeJobType)

	const deadPool = "dead-pool-x"
	conn := pool.Get()
	defer func() { _ = conn.Close() }()

	for _, jt := range []string{job.SampleJob, fakeJobType} {
		payload, err := json.Marshal(map[string]any{"name": jt, "id": "job-of-" + jt, "t": 1, "args": map[string]any{}})
		require.NoError(t, err)
		_, err = conn.Do("LPUSH", rds.KeyInProgressQueue(ns, jt, deadPool), string(payload))
		require.NoError(t, err)
		_, err = conn.Do("SET", rds.KeyJobLock(ns, jt), 1)
		require.NoError(t, err)
		_, err = conn.Do("HSET", rds.KeyJobLockInfo(ns, jt), deadPool, 1)
		require.NoError(t, err)
	}

	require.NoError(t, r.reEnqueueInProgressJobs())

	for _, jt := range []string{job.SampleJob, fakeJobType} {
		n, err := redis.Int(conn.Do("LLEN", rds.KeyJobs(ns, jt)))
		require.NoError(t, err)
		require.Equal(t, 1, n, "dead-pool job of type %s must be requeued", jt)
		lock, err := redis.Int(conn.Do("GET", rds.KeyJobLock(ns, jt)))
		require.NoError(t, err)
		require.Zero(t, lock, "lock of type %s must be released", jt)
	}
}
