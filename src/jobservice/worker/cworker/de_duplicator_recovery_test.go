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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/jobservice/common/utils"
	"github.com/goharbor/harbor/src/jobservice/errs"
	"github.com/goharbor/harbor/src/jobservice/job"
)

// TestUniqueSignEnqueueConflict verifies the unique sign is a SET NX gate:
// only one job with identical name+parameters may pass while a run is alive.
func TestUniqueSignEnqueueConflict(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_dedup_" + utils.MakeIdentifier()[:8]
	dd := NewDeDuplicator(ns, pool)

	params := job.Parameters{"image": "demo:unique"}

	require.NoError(t, dd.MustUnique(job.SampleJob, params), "first enqueue must pass")
	err := dd.MustUnique(job.SampleJob, params)
	require.Error(t, err)
	require.True(t, errs.IsConflictError(err), "duplicate enqueue must be reported as conflict: %v", err)

	// Different parameters are a different unique key.
	require.NoError(t, dd.MustUnique(job.SampleJob, job.Parameters{"image": "demo:other"}),
		"jobs with different parameters do not conflict")
}

// TestUniqueSignReleasedAfterExit verifies the normal completion path releases
// the sign so that the same unique job can be launched again afterwards.
func TestUniqueSignReleasedAfterExit(t *testing.T) {
	_, pool := newMiniRedisPool(t)
	ns := "recovery_dedup_done_" + utils.MakeIdentifier()[:8]
	dd := NewDeDuplicator(ns, pool)

	params := job.Parameters{"image": "demo:released"}

	require.NoError(t, dd.MustUnique(job.SampleJob, params))
	require.NoError(t, dd.DelUniqueSign(job.SampleJob, params))
	require.NoError(t, dd.MustUnique(job.SampleJob, params),
		"after an orderly exit the same unique job can be enqueued again")
}

// TestUniqueSignCrashLeaseBoundary pins the crash-recovery boundary of unique
// jobs: if the worker dies while holding the sign, nobody removes it, so the
// same job stays rejected until the 24h TTL heals the stale lease
// automatically.
func TestUniqueSignCrashLeaseBoundary(t *testing.T) {
	mr, pool := newMiniRedisPool(t)
	ns := "recovery_dedup_crash_" + utils.MakeIdentifier()[:8]
	dd := NewDeDuplicator(ns, pool)

	params := job.Parameters{"image": "demo:crashed"}
	require.NoError(t, dd.MustUnique(job.SampleJob, params))

	signKey, err := redisKeyUniqueJob(ns, job.SampleJob, params)
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, mr.TTL(signKey), "unique sign must carry the 24h recovery TTL")

	// Simulate a restarted process: the sign was never deleted.
	dd2 := NewDeDuplicator(ns, pool)
	require.True(t, errs.IsConflictError(dd2.MustUnique(job.SampleJob, params)),
		"crashed lease blocks duplicates right after restart")

	// After TTL expiry the stale lease self-heals without any reaper action.
	mr.FastForward(24*time.Hour + time.Minute)
	require.NoError(t, dd2.MustUnique(job.SampleJob, params),
		"expired stale lease must stop blocking the unique job")
}
