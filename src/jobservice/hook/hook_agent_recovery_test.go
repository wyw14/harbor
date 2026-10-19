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

package hook

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
	"github.com/goharbor/harbor/src/jobservice/job"
)

func newHookMiniPool(t *testing.T) (*miniredis.Miniredis, *redis.Pool, string) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return redis.Dial("tcp", mr.Addr()) },
	}
	t.Cleanup(func() { _ = pool.Close() })
	ns := "recovery_hook_" + utils.MakeIdentifier()[:8]
	return mr, pool, ns
}

func newTestAgent(pool *redis.Pool, ns string, client Client) *basicAgent {
	return &basicAgent{
		context:   context.Background(),
		namespace: ns,
		client:    client,
		redisPool: pool,
		tokens:    make(chan struct{}, 1),
	}
}

func seedAckStats(t *testing.T, pool *redis.Pool, ns, jid string, revision int64) {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	_, err := conn.Do("HMSET", rds.KeyJobStats(ns, jid),
		"id", jid,
		"name", job.SampleJob,
		"status", job.RunningStatus.String(),
		"revision", revision,
		"update_time", time.Now().Unix(),
	)
	require.NoError(t, err)
}

func ackEvent(jid, status string, revision, checkInAt int64) *Event {
	return &Event{
		URL:       "http://harbor-core/service/notifications/tasks/1",
		Message:   "status change",
		Timestamp: time.Now().Unix(),
		Data: &job.StatusChange{
			JobID:  jid,
			Status: status,
			Metadata: &job.StatsInfo{
				Revision:  revision,
				CheckInAt: checkInAt,
			},
		},
	}
}

func readAck(t *testing.T, pool *redis.Pool, ns, jid string) *job.ACK {
	t.Helper()
	conn := pool.Get()
	defer func() { _ = conn.Close() }()
	raw, err := redis.Bytes(conn.Do("HGET", rds.KeyJobStats(ns, jid), "ack"))
	require.NoError(t, err)
	ack := &job.ACK{}
	require.NoError(t, json.Unmarshal(raw, ack))
	return ack
}

// fakeHookClient records delivery attempts instead of performing HTTP calls.
type fakeHookClient struct {
	mu      sync.Mutex
	sent    int
	failNow bool
}

func (c *fakeHookClient) SendEvent(_ *Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent++
	if c.failNow {
		return context.DeadlineExceeded
	}
	return nil
}

func (c *fakeHookClient) deliveries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
}

// TestTriggerWritesAck verifies a successfully delivered status event is
// followed by an atomic ACK that also releases the final-status share of the
// in-progress counter.
func TestTriggerWritesAck(t *testing.T) {
	_, pool, ns := newHookMiniPool(t)
	jid := "hook-ack-" + utils.MakeIdentifier()[:10]
	seedAckStats(t, pool, ns, jid, 100)

	conn := pool.Get()
	_, err := conn.Do("HSET", rds.KeyJobTrackInProgress(ns), jid, 1)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	client := &fakeHookClient{}
	ba := newTestAgent(pool, ns, client)

	require.NoError(t, ba.Trigger(ackEvent(jid, job.SuccessStatus.String(), 100, 0)))
	require.Equal(t, 1, client.deliveries())

	ack := readAck(t, pool, ns, jid)
	require.Equal(t, job.SuccessStatus.String(), ack.Status)
	require.Equal(t, int64(100), ack.Revision)

	// Final ACK releases the remaining in-progress counter.
	conn = pool.Get()
	defer func() { _ = conn.Close() }()
	_, err = redis.Int(conn.Do("HGET", rds.KeyJobTrackInProgress(ns), jid))
	require.Equal(t, redis.ErrNil, err, "ACK must drop the in-progress track entry")
}

// TestDuplicateAckIsIdempotent verifies a redelivered duplicate ACK is a
// no-op: the same revision/status is accepted without rewriting the record and
// without touching the released in-progress counter.
func TestDuplicateAckIsIdempotent(t *testing.T) {
	_, pool, ns := newHookMiniPool(t)
	jid := "hook-dup-" + utils.MakeIdentifier()[:10]
	seedAckStats(t, pool, ns, jid, 100)

	conn := pool.Get()
	_, err := conn.Do("HSET", rds.KeyJobTrackInProgress(ns), jid, 1)
	require.NoError(t, err)

	ba := newTestAgent(pool, ns, &fakeHookClient{})
	evt := ackEvent(jid, job.SuccessStatus.String(), 100, 0)
	require.NoError(t, ba.ack(evt))

	first := readAck(t, pool, ns, jid)
	require.NoError(t, ba.ack(evt), "duplicate ACK must stay idempotent")
	second := readAck(t, pool, ns, jid)
	require.Equal(t, first, second, "duplicate ACK must not mutate the record")

	// The second ACK must not resurrect or decrement the counter again.
	_, err = redis.Int(conn.Do("HGET", rds.KeyJobTrackInProgress(ns), jid))
	require.Equal(t, redis.ErrNil, err)
	require.NoError(t, conn.Close())
}

// TestStaleAckRejected verifies an ACK carrying an older revision than the
// one already recorded is refused, so late callbacks of a previous attempt can
// never overwrite the newer delivery state.
func TestStaleAckRejected(t *testing.T) {
	_, pool, ns := newHookMiniPool(t)
	jid := "hook-stale-" + utils.MakeIdentifier()[:10]
	seedAckStats(t, pool, ns, jid, 200)
	// Current ACK is already Success@200 (e.g. the retried attempt finished).
	ba := newTestAgent(pool, ns, &fakeHookClient{})
	require.NoError(t, ba.ack(ackEvent(jid, job.SuccessStatus.String(), 200, 0)))

	err := ba.ack(ackEvent(jid, job.SuccessStatus.String(), 100, 0))
	require.Error(t, err, "older revision must not be ACKed")
	require.Contains(t, err.Error(), "no ack done")

	ack := readAck(t, pool, ns, jid)
	require.Equal(t, int64(200), ack.Revision, "newer ACK state must survive a stale duplicate")
}

// TestOutdatedCallbackSuppression is the table-driven matrix the retry loop
// consults before every resend: only events at or behind the ACKed watermark
// (revision -> status code -> check-in timestamp) are dropped.
func TestOutdatedCallbackSuppression(t *testing.T) {
	_, pool, ns := newHookMiniPool(t)
	jid := "hook-outdated-" + utils.MakeIdentifier()[:10]
	seedAckStats(t, pool, ns, jid, 100)
	ba := newTestAgent(pool, ns, &fakeHookClient{})

	cases := []struct {
		name      string
		ackStatus string
		ackRev    int64
		ackCheck  int64
		evtStatus string
		evtRev    int64
		evtCheck  int64
		outdated  bool
	}{
		{"identical duplicate delivery", job.SuccessStatus.String(), 100, 0, job.SuccessStatus.String(), 100, 0, true},
		{"event from an older attempt (revision behind)", job.SuccessStatus.String(), 100, 0, job.SuccessStatus.String(), 90, 0, true},
		{"stale Running after final ACK", job.SuccessStatus.String(), 100, 0, job.RunningStatus.String(), 100, 0, true},
		{"duplicate check-in at same watermark", job.RunningStatus.String(), 100, 5, job.RunningStatus.String(), 100, 5, true},
		{"older check-in than watermark", job.RunningStatus.String(), 100, 5, job.RunningStatus.String(), 100, 3, true},
		{"newer attempt (revision ahead)", job.SuccessStatus.String(), 100, 0, job.SuccessStatus.String(), 200, 0, false},
		{"newer check-in progress", job.RunningStatus.String(), 100, 5, job.RunningStatus.String(), 100, 10, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset ACK watermark per case.
			conn := pool.Get()
			ackRaw, _ := json.Marshal(&job.ACK{Status: tc.ackStatus, Revision: tc.ackRev, CheckInAt: tc.ackCheck})
			_, err := conn.Do("HSET", rds.KeyJobStats(ns, jid), "ack", string(ackRaw))
			require.NoError(t, err)
			require.NoError(t, conn.Close())

			got, err := ba.isOutdated(ackEvent(jid, tc.evtStatus, tc.evtRev, tc.evtCheck))
			require.NoError(t, err)
			require.Equal(t, tc.outdated, got)
		})
	}
}

// TestFailedDeliveryKeepsNoAck verifies a failed first delivery leaves no ACK
// behind, so the background retry path (and ultimately the reaper) still has a
// reason to resend the event.
func TestFailedDeliveryKeepsNoAck(t *testing.T) {
	_, pool, ns := newHookMiniPool(t)
	jid := "hook-fail-" + utils.MakeIdentifier()[:10]
	seedAckStats(t, pool, ns, jid, 100)

	client := &fakeHookClient{failNow: true}
	ba := newTestAgent(pool, ns, client)
	require.Error(t, ba.Trigger(ackEvent(jid, job.SuccessStatus.String(), 100, 0)))

	conn := pool.Get()
	_, err := redis.Bytes(conn.Do("HGET", rds.KeyJobStats(ns, jid), "ack"))
	require.Equal(t, redis.ErrNil, err, "no ACK may be persisted before a successful delivery")
	require.NoError(t, conn.Close())

	// Release the retry goroutine started by Trigger so it can terminate after
	// its backoff instead of running to the 5m ceiling.
	client.mu.Lock()
	client.failNow = false
	client.mu.Unlock()
}
