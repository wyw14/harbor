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

package jobmonitor

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	jobSvc "github.com/goharbor/harbor/src/jobservice/job"
	jm "github.com/goharbor/harbor/src/pkg/jobmonitor"
	"github.com/goharbor/harbor/src/pkg/task"
	taskDao "github.com/goharbor/harbor/src/pkg/task/dao"
	monitorMock "github.com/goharbor/harbor/src/testing/pkg/jobmonitor"
	taskMock "github.com/goharbor/harbor/src/testing/pkg/task"
)

// recoveryExecutionDAO embeds the real interface so only RefreshStatus has to
// be faked for the partial-commit scenarios.
type recoveryExecutionDAO struct {
	taskDao.ExecutionDAO
	mu        sync.Mutex
	refreshed []int64
	failIDs   map[int64]bool
}

func (d *recoveryExecutionDAO) RefreshStatus(_ context.Context, id int64) (bool, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refreshed = append(d.refreshed, id)
	if d.failIDs[id] {
		return false, "", errors.New("refresh failed")
	}
	return true, jobSvc.SuccessStatus.String(), nil
}

func (d *recoveryExecutionDAO) refreshedIDs() []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]int64, len(d.refreshed))
	copy(out, d.refreshed)
	return out
}

func newRecoveryController(t *testing.T) (*monitorController, *taskMock.Manager, *monitorMock.RedisClient, *recoveryExecutionDAO) {
	t.Helper()
	taskMgr := &taskMock.Manager{}
	redisCli := &monitorMock.RedisClient{}
	execDAO := &recoveryExecutionDAO{failIDs: map[int64]bool{}}
	ctl := &monitorController{
		taskManager:           taskMgr,
		jobServiceRedisClient: func() (jm.RedisClient, error) { return redisCli, nil },
		executionDAO:          execDAO,
	}
	return ctl, taskMgr, redisCli, execDAO
}

// TestGetJobLogResolvesByJobID pins the log-retrieval boundary after recovery:
// execution -> task row -> jobservice JobID is the stable identity. Queue
// re-delivery never changes the JobID (see worker reaper tests), so a task log
// stays fetchable through the same key even when the jobservice restarted.
func TestGetJobLogResolvesByJobID(t *testing.T) {
	ctl, taskMgr, _, _ := newRecoveryController(t)
	want := []byte("job log line 1\njob log line 2\n")
	taskMgr.On("GetLogByJobID", mock.Anything, "job-stable-id").Return(want, nil).Once()

	got, err := ctl.GetJobLog(context.Background(), "job-stable-id")
	require.NoError(t, err)
	require.Equal(t, want, got)
	taskMgr.AssertExpectations(t)
}

// TestStopRunningJobAlreadyComplete verifies cancelling a job whose task row is
// already gone (completed and swept) is a successful no-op instead of an error:
// the operator action "stop" has the same end state and must not loop.
func TestStopRunningJobAlreadyComplete(t *testing.T) {
	ctl, taskMgr, _, _ := newRecoveryController(t)
	taskMgr.On("List", mock.Anything, mock.Anything).Return([]*task.Task{}, nil).Once()

	require.NoError(t, ctl.stopJob(context.Background(), "job-vanished"))
	taskMgr.AssertNotCalled(t, "Stop", mock.Anything, mock.Anything)
}

// TestStopRunningJobDuplicateTaskRows verifies ambiguous job_id mappings fail
// loudly rather than stopping an unrelated task.
func TestStopRunningJobDuplicateTaskRows(t *testing.T) {
	ctl, taskMgr, _, _ := newRecoveryController(t)
	taskMgr.On("List", mock.Anything, mock.Anything).Return([]*task.Task{
		{ID: 1, JobID: "job-x"},
		{ID: 2, JobID: "job-x"},
	}, nil).Once()

	require.Error(t, ctl.stopJob(context.Background(), "job-x"))
	taskMgr.AssertNotCalled(t, "Stop", mock.Anything, mock.Anything)
}

// TestStopAllRunningJobsToleratesCompletedMembers verifies bulk cancel does
// not abort when a worker-reported job has already completed by the time its
// task is looked up.
func TestStopAllRunningJobsToleratesCompletedMembers(t *testing.T) {
	ctl, taskMgr, _, _ := newRecoveryController(t)

	// Empty lookup means the job finished before cancel was processed; both
	// members must resolve as no-ops without invoking task.Stop.
	taskMgr.On("List", mock.Anything, mock.Anything).Return([]*task.Task{}, nil).Twice()
	require.NoError(t, ctl.stopJob(context.Background(), "job-finished"))
	require.NoError(t, ctl.stopJob(context.Background(), "job-running"))
	taskMgr.AssertNotCalled(t, "Stop", mock.Anything, mock.Anything)
}

// TestUpdateJobStatusRefreshesEveryExecution verifies the DB reconciliation
// after a pending-queue cancel: every pending execution of the vendor type is
// re-aggregated, and one bad execution refresh does not stop the others.
func TestUpdateJobStatusRefreshesEveryExecution(t *testing.T) {
	ctl, taskMgr, _, execDAO := newRecoveryController(t)
	const vendor = "REPLICATION"

	taskMgr.On("ExecutionIDsByVendorAndStatus", mock.Anything, vendor, jobSvc.PendingStatus.String()).
		Return([]int64{101, 102, 103}, nil).Once()
	taskMgr.On("UpdateStatusInBatch", mock.Anything, []string{"j1", "j2"}, jobSvc.StoppedStatus.String(), batchUpdateSize).
		Return(nil).Once()
	execDAO.failIDs[102] = true // partial failure while refreshing executions

	require.NoError(t, ctl.updateJobStatusInTask(context.Background(), vendor, []string{"j1", "j2"}, jobSvc.StoppedStatus.String()))
	require.Equal(t, []int64{101, 102, 103}, execDAO.refreshedIDs(),
		"a single refresh failure must not skip the remaining executions")
}

// TestBatchTaskUpdateFailureDoesNotBlockExecutionRefresh verifies the two
// database effects are best-effort independent: failing to flip the task rows
// still triggers execution status aggregation so the vendor-facing state
// converges.
func TestBatchTaskUpdateFailureDoesNotBlockExecutionRefresh(t *testing.T) {
	ctl, taskMgr, _, execDAO := newRecoveryController(t)
	const vendor = "GARBAGE_COLLECTION"

	taskMgr.On("ExecutionIDsByVendorAndStatus", mock.Anything, vendor, jobSvc.PendingStatus.String()).
		Return([]int64{201}, nil).Once()
	taskMgr.On("UpdateStatusInBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("batch update failed")).Once()

	require.NoError(t, ctl.updateJobStatusInTask(context.Background(), vendor, []string{"j9"}, jobSvc.StoppedStatus.String()))
	require.Equal(t, []int64{201}, execDAO.refreshedIDs(),
		"execution refresh must run even when the batch task update failed")
}

// TestPendingExecutionLookupErrorPropagates verifies a genuine lookup failure
// is reported rather than silently swallowed.
func TestPendingExecutionLookupErrorPropagates(t *testing.T) {
	ctl, taskMgr, _, _ := newRecoveryController(t)
	taskMgr.On("ExecutionIDsByVendorAndStatus", mock.Anything, mock.Anything, mock.Anything).
		Return([]int64(nil), errors.New("db unavailable")).Once()

	err := ctl.updateJobStatusInTask(context.Background(), "REPLICATION", []string{"j1"}, jobSvc.StoppedStatus.String())
	require.Error(t, err)
	taskMgr.AssertNotCalled(t, "UpdateStatusInBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestStopPendingJobsPropagatesRedisFailure verifies a Redis-side cancel
// failure surfaces to the caller instead of being reported as success.
func TestStopPendingJobsPropagatesRedisFailure(t *testing.T) {
	ctl, _, redisCli, _ := newRecoveryController(t)
	redisCli.On("StopPendingJobs", mock.Anything, "REPLICATION").
		Return([]string{}, errors.New("redis down")).Once()

	require.Error(t, ctl.stopPendingJob(context.Background(), "REPLICATION"))
}
