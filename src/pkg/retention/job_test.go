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

package retention

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/jobservice/logger"
	"github.com/goharbor/harbor/src/lib/selector"
	"github.com/goharbor/harbor/src/lib/selector/selectors/doublestar"
	"github.com/goharbor/harbor/src/pkg/retention/dep"
	"github.com/goharbor/harbor/src/pkg/retention/policy"
	"github.com/goharbor/harbor/src/pkg/retention/policy/action"
	"github.com/goharbor/harbor/src/pkg/retention/policy/lwp"
	"github.com/goharbor/harbor/src/pkg/retention/policy/rule"
	"github.com/goharbor/harbor/src/pkg/retention/policy/rule/latestps"
)

// JobTestSuite is test suite for testing job
type JobTestSuite struct {
	suite.Suite

	oldClient dep.Client
}

// TestJob is entry of running JobTestSuite
func TestJob(t *testing.T) {
	suite.Run(t, new(JobTestSuite))
}

// SetupSuite ...
func (suite *JobTestSuite) SetupSuite() {
	suite.oldClient = dep.DefaultClient
	dep.DefaultClient = &fakeRetentionClient{}
}

// TearDownSuite ...
func (suite *JobTestSuite) TearDownSuite() {
	dep.DefaultClient = suite.oldClient
}

func (suite *JobTestSuite) TestRunSuccess() {
	params := make(job.Parameters)
	params[ParamDryRun] = false
	repository := &selector.Repository{
		Namespace: "library",
		Name:      "harbor",
		Kind:      selector.Image,
	}
	repoJSON, err := repository.ToJSON()
	require.Nil(suite.T(), err)
	params[ParamRepo] = repoJSON

	scopeSelectors := make(map[string][]*rule.Selector)
	scopeSelectors["project"] = []*rule.Selector{{
		Kind:       doublestar.Kind,
		Decoration: doublestar.RepoMatches,
		Pattern:    "{harbor}",
	}}

	ruleParams := make(rule.Parameters)
	ruleParams[latestps.ParameterK] = 10

	meta := &lwp.Metadata{
		Algorithm: policy.AlgorithmOR,
		Rules: []*rule.Metadata{
			{
				ID:         1,
				Priority:   999,
				Action:     action.Retain,
				Template:   latestps.TemplateID,
				Parameters: ruleParams,
				TagSelectors: []*rule.Selector{{
					Kind:       doublestar.Kind,
					Decoration: doublestar.Matches,
					Pattern:    "**",
				}},
				ScopeSelectors: scopeSelectors,
			},
		},
	}
	metaJSON, err := meta.ToJSON()
	require.Nil(suite.T(), err)
	params[ParamMeta] = metaJSON

	j := &Job{}
	err = j.Validate(params)
	require.NoError(suite.T(), err)

	err = j.Run(&fakeJobContext{}, params)
	require.NoError(suite.T(), err)
}

// buildJobParams builds the job parameters with the latestPushedK=1 rule,
// which selects 1 of the 2 fake candidates as the deletion candidate
func (suite *JobTestSuite) buildJobParams(dryRun bool, threshold *int) job.Parameters {
	params := make(job.Parameters)
	params[ParamDryRun] = dryRun
	repository := &selector.Repository{
		Namespace: "library",
		Name:      "harbor",
		Kind:      selector.Image,
	}
	repoJSON, err := repository.ToJSON()
	require.Nil(suite.T(), err)
	params[ParamRepo] = repoJSON

	ruleParams := make(rule.Parameters)
	ruleParams[latestps.ParameterK] = 1

	meta := &lwp.Metadata{
		Algorithm: policy.AlgorithmOR,
		Rules: []*rule.Metadata{
			{
				ID:       1,
				Priority: 999,
				Action:   action.Retain,
				Template: latestps.TemplateID,
				TagSelectors: []*rule.Selector{{
					Kind:       doublestar.Kind,
					Decoration: doublestar.Matches,
					Pattern:    "**",
				}},
				Parameters: ruleParams,
			},
		},
		MaxDeletionsPerRepository: threshold,
	}
	metaJSON, err := meta.ToJSON()
	require.Nil(suite.T(), err)
	params[ParamMeta] = metaJSON

	return params
}

type checkinReport struct {
	Total                     int     `json:"total"`
	Retained                  int     `json:"retained"`
	DryRun                    bool    `json:"dry_run"`
	Deleted                   []any   `json:"deleted"`
	DeletionCandidates        int     `json:"deletion_candidates"`
	MaxDeletionsPerRepository *int    `json:"max_deletions_per_repository"`
	DeletionBlocked           bool    `json:"deletion_blocked"`
}

// TestRunBlockedRealRun verifies a real run blocked by the protection
// threshold deletes zero artifact, reports the block and ends with an error
// so that the execution is not reported as all succeed.
func (suite *JobTestSuite) TestRunBlockedRealRun() {
	zero := 0
	params := suite.buildJobParams(false, &zero)

	j := &Job{}
	require.NoError(suite.T(), j.Validate(params))

	jobCtx := &fakeJobContext{}
	err := j.Run(jobCtx, params)
	require.Error(suite.T(), err)

	report := &checkinReport{}
	require.NoError(suite.T(), json.Unmarshal([]byte(jobCtx.checkin), report))
	assert.True(suite.T(), report.DeletionBlocked)
	assert.Equal(suite.T(), 1, report.DeletionCandidates)
	require.NotNil(suite.T(), report.MaxDeletionsPerRepository)
	assert.Equal(suite.T(), 0, *report.MaxDeletionsPerRepository)
	assert.Empty(suite.T(), report.Deleted)
	assert.Equal(suite.T(), 2, report.Retained)
}

// TestRunBlockedDryRun verifies a dry run uses the same block judgment,
// reports the would-be block and keeps zero deletion without failing the job.
func (suite *JobTestSuite) TestRunBlockedDryRun() {
	zero := 0
	params := suite.buildJobParams(true, &zero)

	j := &Job{}
	require.NoError(suite.T(), j.Validate(params))

	jobCtx := &fakeJobContext{}
	require.NoError(suite.T(), j.Run(jobCtx, params))

	report := &checkinReport{}
	require.NoError(suite.T(), json.Unmarshal([]byte(jobCtx.checkin), report))
	assert.True(suite.T(), report.DeletionBlocked)
	assert.Equal(suite.T(), 1, report.DeletionCandidates)
	assert.Empty(suite.T(), report.Deleted)
}

// TestRunNotBlocked verifies the original deletion flow proceeds when the
// candidate count is within the threshold.
func (suite *JobTestSuite) TestRunNotBlocked() {
	ten := 10
	params := suite.buildJobParams(false, &ten)

	j := &Job{}
	require.NoError(suite.T(), j.Validate(params))

	jobCtx := &fakeJobContext{}
	require.NoError(suite.T(), j.Run(jobCtx, params))

	report := &checkinReport{}
	require.NoError(suite.T(), json.Unmarshal([]byte(jobCtx.checkin), report))
	assert.False(suite.T(), report.DeletionBlocked)
	assert.Equal(suite.T(), 1, report.DeletionCandidates)
	require.NotNil(suite.T(), report.MaxDeletionsPerRepository)
	assert.Equal(suite.T(), 10, *report.MaxDeletionsPerRepository)
	assert.Len(suite.T(), report.Deleted, 1)
}

type fakeRetentionClient struct{}

// GetCandidates ...
func (frc *fakeRetentionClient) GetCandidates(repo *selector.Repository) ([]*selector.Candidate, error) {
	return []*selector.Candidate{
		{
			Namespace:    "library",
			Repository:   "harbor",
			Kind:         "image",
			Tags:         []string{"latest"},
			Digest:       "latest",
			PushedTime:   time.Now().Unix() - 11,
			PulledTime:   time.Now().Unix() - 2,
			CreationTime: time.Now().Unix() - 10,
			Labels:       []string{"L1", "L2"},
		},
		{
			Namespace:    "library",
			Repository:   "harbor",
			Kind:         "image",
			Tags:         []string{"dev", "test"},
			Digest:       "dev",
			PushedTime:   time.Now().Unix() - 10,
			PulledTime:   time.Now().Unix() - 3,
			CreationTime: time.Now().Unix() - 20,
			Labels:       []string{"L3"},
		},
	}, nil
}

// Delete ...
func (frc *fakeRetentionClient) Delete(candidate *selector.Candidate) error {
	return nil
}

// SubmitTask ...
func (frc *fakeRetentionClient) DeleteRepository(repo *selector.Repository) error {
	return nil
}

type fakeLogger struct{}

// For debuging
func (l *fakeLogger) Debug(v ...any) {}

// For debuging with format
func (l *fakeLogger) Debugf(format string, v ...any) {}

// For logging info
func (l *fakeLogger) Info(v ...any) {
	fmt.Println(v...)
}

// For logging info with format
func (l *fakeLogger) Infof(format string, v ...any) {
	fmt.Printf(format+"\n", v...)
}

// For warning
func (l *fakeLogger) Warning(v ...any) {}

// For warning with format
func (l *fakeLogger) Warningf(format string, v ...any) {}

// For logging error
func (l *fakeLogger) Error(v ...any) {
	fmt.Println(v...)
}

// For logging error with format
func (l *fakeLogger) Errorf(format string, v ...any) {
}

// For fatal error
func (l *fakeLogger) Fatal(v ...any) {}

// For fatal error with error
func (l *fakeLogger) Fatalf(format string, v ...any) {}

type fakeJobContext struct {
	checkin string
}

func (c *fakeJobContext) Build(tracker job.Tracker) (job.Context, error) {
	return nil, nil
}

func (c *fakeJobContext) Get(prop string) (any, bool) {
	return nil, false
}

func (c *fakeJobContext) SystemContext() context.Context {
	return context.TODO()
}

func (c *fakeJobContext) Checkin(status string) error {
	fmt.Printf("Check in: %s\n", status)
	c.checkin = status

	return nil
}

func (c *fakeJobContext) OPCommand() (job.OPCommand, bool) {
	return "", false
}

func (c *fakeJobContext) GetLogger() logger.Interface {
	return &fakeLogger{}
}

func (c *fakeJobContext) Tracker() job.Tracker {
	return nil
}
