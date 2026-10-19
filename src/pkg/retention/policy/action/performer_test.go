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

package action

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/common/dao"
	"github.com/goharbor/harbor/src/controller/immutable"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/selector"
	immumodel "github.com/goharbor/harbor/src/pkg/immutable/model"
	"github.com/goharbor/harbor/src/pkg/retention/dep"
)

// TestPerformerSuite tests the performer related function
type TestPerformerSuite struct {
	suite.Suite

	oldClient dep.Client
	all       []*selector.Candidate
}

// TestPerformer is the entry of the TestPerformerSuite
func TestPerformer(t *testing.T) {
	suite.Run(t, new(TestPerformerSuite))
}

// SetupSuite ...
func (suite *TestPerformerSuite) SetupSuite() {
	suite.all = []*selector.Candidate{
		{
			Namespace:  "library",
			Repository: "harbor",
			Kind:       "image",
			Tags:       []string{"latest"},
			Digest:     "latest",
			PushedTime: time.Now().Unix(),
			Labels:     []string{"L1", "L2"},
		},
		{
			Namespace:  "library",
			Repository: "harbor",
			Kind:       "image",
			Tags:       []string{"dev"},
			Digest:     "dev",
			PushedTime: time.Now().Unix(),
			Labels:     []string{"L3"},
		},
	}

	suite.oldClient = dep.DefaultClient
	dep.DefaultClient = &fakeRetentionClient{}
	dao.PrepareTestForPostgresSQL()
}

// TearDownSuite ...
func (suite *TestPerformerSuite) TearDownSuite() {
	dep.DefaultClient = suite.oldClient
}

// TestPerform tests Perform action
func (suite *TestPerformerSuite) TestPerform() {
	p := &retainAction{
		all: suite.all,
	}

	candidates := []*selector.Candidate{
		{
			Namespace:  "library",
			Repository: "harbor",
			Kind:       "image",
			Tags:       []string{"latest"},
			Digest:     "latest",
			PushedTime: time.Now().Unix(),
			Labels:     []string{"L1", "L2"},
		},
	}

	results, err := p.Perform(orm.Context(), candidates)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), 1, len(results))
	require.NotNil(suite.T(), results[0].Target)
	assert.NoError(suite.T(), results[0].Error)
	assert.Equal(suite.T(), "dev", results[0].Target.Tags[0])
}

// TestPerform tests Perform action
func (suite *TestPerformerSuite) TestPerformImmutable() {
	all := []*selector.Candidate{
		{
			NamespaceID: 1,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        "image",
			Tags:        []string{"latest"},
			Digest:      "d0",
			PushedTime:  time.Now().Unix(),
			Labels:      []string{"L1", "L2"},
		},
		{
			NamespaceID: 1,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        "image",
			Tags:        []string{"dev"},
			Digest:      "d1",
			PushedTime:  time.Now().Unix(),
			Labels:      []string{"L3"},
		},
		{
			NamespaceID: 1,
			Namespace:   "library",
			Repository:  "test",
			Kind:        "image",
			Tags:        []string{"immute"},
			Digest:      "d2",
			PushedTime:  time.Now().Unix(),
			Labels:      []string{"L1", "L2"},
		},
		{
			NamespaceID: 1,
			Namespace:   "library",
			Repository:  "test",
			Kind:        "image",
			Tags:        []string{"samedig"},
			Digest:      "d2",
			PushedTime:  time.Now().Unix(),
			Labels:      []string{"L1", "L2"},
		},
	}
	p := &retainAction{
		all: all,
	}

	rule := &immumodel.Metadata{
		ProjectID: 1,
		Priority:  1,
		Action:    "immutable",
		Template:  "immutable_template",
		TagSelectors: []*immumodel.Selector{
			{
				Kind:       "doublestar",
				Decoration: "matches",
				Pattern:    "immute",
			},
		},
		ScopeSelectors: map[string][]*immumodel.Selector{
			"repository": {
				{
					Kind:       "doublestar",
					Decoration: "repoMatches",
					Pattern:    "**",
				},
			},
		},
	}
	imid, e := immutable.Ctr.CreateImmutableRule(orm.Context(), rule)
	assert.NoError(suite.T(), e)
	defer func() {
		assert.NoError(suite.T(), immutable.Ctr.DeleteImmutableRule(orm.Context(), imid))
	}()

	candidates := []*selector.Candidate{
		{
			NamespaceID: 1,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        "image",
			Tags:        []string{"latest"},
			Digest:      "d0",
			PushedTime:  time.Now().Unix(),
			Labels:      []string{"L1", "L2"},
		},
	}

	results, err := p.Perform(orm.Context(), candidates)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), 3, len(results))
	for _, r := range results {
		require.NotNil(suite.T(), r.Target)
		if r.Target.Digest == "d1" {
			require.NoError(suite.T(), r.Error)
			require.Equal(suite.T(), "dev", r.Target.Tags[0])
		} else if r.Target.Digest == "d2" {
			require.Error(suite.T(), r.Error)
			require.IsType(suite.T(), (*selector.ImmutableError)(nil), r.Error)
		} else {
			require.Fail(suite.T(), "should not delete "+r.Target.Hash())
		}
	}
	require.NotNil(suite.T(), results[0].Target)
	assert.NoError(suite.T(), results[0].Error)
	assert.Equal(suite.T(), "dev", results[0].Target.Tags[0])
}

func (suite *TestPerformerSuite) TestPerformImmutableRepositoryExclusion() {
	const projectID int64 = 987654321
	rule := &immumodel.Metadata{
		ProjectID: projectID,
		Priority:  1,
		Action:    "immutable",
		Template:  "immutable_template",
		TagSelectors: []*immumodel.Selector{
			{
				Kind:       "doublestar",
				Decoration: "matches",
				Pattern:    "**",
			},
		},
		ScopeSelectors: map[string][]*immumodel.Selector{
			"repository": {
				{
					Kind:       "doublestar",
					Decoration: "repoExcludes",
					Pattern:    "qa/**",
				},
			},
		},
	}
	ruleID, err := immutable.Ctr.CreateImmutableRule(orm.Context(), rule)
	require.NoError(suite.T(), err)
	defer func() {
		require.NoError(suite.T(), immutable.Ctr.DeleteImmutableRule(orm.Context(), ruleID))
	}()

	p := &retainAction{
		all: []*selector.Candidate{
			{
				NamespaceID: projectID,
				Namespace:   "example",
				Repository:  "qa/team/component/backend",
				Kind:        selector.Image,
				Tags:        []string{"build-42"},
				Digest:      "d0",
			},
			{
				NamespaceID: projectID,
				Namespace:   "example",
				Repository:  "prod/team/component/backend",
				Kind:        selector.Image,
				Tags:        []string{"build-42"},
				Digest:      "d1",
			},
		},
	}

	results, err := p.Perform(orm.Context(), nil)
	require.NoError(suite.T(), err)
	require.Len(suite.T(), results, 2)
	assert.NoError(suite.T(), results[0].Error)
	assert.IsType(suite.T(), (*selector.ImmutableError)(nil), results[1].Error)
}

// intPtr returns a pointer to the given int
func intPtr(n int) *int {
	return &n
}

// runWithCountingClient runs the retain action with a client recording the
// deleted artifact digests
func (suite *TestPerformerSuite) runWithCountingClient(threshold *int, isDryRun bool,
	all, retained []*selector.Candidate) ([]*selector.Result, *fakeRetentionClient) {
	frc := &fakeRetentionClient{}
	old := dep.DefaultClient
	dep.DefaultClient = frc
	defer func() { dep.DefaultClient = old }()

	p := &retainAction{
		all:                       all,
		isDryRun:                  isDryRun,
		maxDeletionsPerRepository: threshold,
	}
	results, err := p.Perform(orm.Context(), retained)
	require.NoError(suite.T(), err)
	return results, frc
}

// blockTestCandidates builds n candidates with unique digests under a project
// without any immutable rule
func blockTestCandidates(n int) []*selector.Candidate {
	all := make([]*selector.Candidate, 0, n)
	for i := 0; i < n; i++ {
		all = append(all, &selector.Candidate{
			NamespaceID: 987654323,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        selector.Image,
			Tags:        []string{fmt.Sprintf("tag-%d", i)},
			Digest:      fmt.Sprintf("digest-%d", i),
			PushedTime:  time.Now().Unix(),
		})
	}
	return all
}

// TestPerformBlockedByMaxDeletions verifies the whole repository is kept when
// the deletion candidate count is greater than the threshold
func (suite *TestPerformerSuite) TestPerformBlockedByMaxDeletions() {
	all := blockTestCandidates(3)
	// retain the first one, the other 2 are deletion candidates
	results, frc := suite.runWithCountingClient(intPtr(1), false, all, all[:1])

	require.Len(suite.T(), results, 2)
	for _, r := range results {
		blockedErr, ok := r.Error.(*selector.MaxDeletionsExceededError)
		require.True(suite.T(), ok)
		assert.Equal(suite.T(), 2, blockedErr.CandidateCount)
		assert.Equal(suite.T(), 1, blockedErr.Max)
	}
	// zero artifact is deleted
	assert.Empty(suite.T(), frc.deleted)
}

// TestPerformEqualThresholdProceeds verifies the deletion flow proceeds when
// the deletion candidate count is equal to the threshold
func (suite *TestPerformerSuite) TestPerformEqualThresholdProceeds() {
	all := blockTestCandidates(3)
	results, frc := suite.runWithCountingClient(intPtr(2), false, all, all[:1])

	require.Len(suite.T(), results, 2)
	for _, r := range results {
		assert.NoError(suite.T(), r.Error)
	}
	assert.ElementsMatch(suite.T(), []string{"digest-1", "digest-2"}, frc.deleted)
}

// TestPerformZeroThreshold verifies threshold 0 blocks any deletion candidate
// but allows a repository without deletion candidate to pass
func (suite *TestPerformerSuite) TestPerformZeroThreshold() {
	all := blockTestCandidates(2)

	// one deletion candidate -> blocked, zero deletion
	results, frc := suite.runWithCountingClient(intPtr(0), false, all, all[:1])
	require.Len(suite.T(), results, 1)
	_, ok := results[0].Error.(*selector.MaxDeletionsExceededError)
	require.True(suite.T(), ok)
	assert.Empty(suite.T(), frc.deleted)

	// all artifacts retained -> no deletion candidate, the round proceeds
	results, frc = suite.runWithCountingClient(intPtr(0), false, all, all)
	assert.Empty(suite.T(), results)
	assert.Empty(suite.T(), frc.deleted)
}

// TestPerformBlockedDryRun verifies dry-run reports the block and keeps zero
// deletion
func (suite *TestPerformerSuite) TestPerformBlockedDryRun() {
	all := blockTestCandidates(2)
	results, frc := suite.runWithCountingClient(intPtr(0), true, all, all[:1])

	require.Len(suite.T(), results, 1)
	_, ok := results[0].Error.(*selector.MaxDeletionsExceededError)
	require.True(suite.T(), ok)
	assert.Empty(suite.T(), frc.deleted)
}

// TestPerformBlockedCountUniqueDigests verifies candidates are counted by
// unique artifact digest: multiple candidates with the same digest count once
func (suite *TestPerformerSuite) TestPerformBlockedCountUniqueDigests() {
	all := []*selector.Candidate{
		{
			NamespaceID: 987654324,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        selector.Image,
			Tags:        []string{"tag-a"},
			Digest:      "shared-digest",
			PushedTime:  time.Now().Unix(),
		},
		{
			NamespaceID: 987654324,
			Namespace:   "library",
			Repository:  "harbor",
			Kind:        selector.Image,
			Tags:        []string{"tag-b"},
			Digest:      "shared-digest",
			PushedTime:  time.Now().Unix(),
		},
	}
	// unique digest count is 1, threshold 1 -> 1 <= 1, the deletion proceeds
	results, frc := suite.runWithCountingClient(intPtr(1), false, all, nil)
	require.Len(suite.T(), results, 2)
	for _, r := range results {
		assert.NoError(suite.T(), r.Error)
	}
	assert.Equal(suite.T(), []string{"shared-digest", "shared-digest"}, frc.deleted)

	// unique digest count is 1, threshold 0 -> 1 > 0, the repository is blocked
	results, frc = suite.runWithCountingClient(intPtr(0), false, all, nil)
	require.Len(suite.T(), results, 2)
	for _, r := range results {
		blockedErr, ok := r.Error.(*selector.MaxDeletionsExceededError)
		require.True(suite.T(), ok)
		assert.Equal(suite.T(), 1, blockedErr.CandidateCount)
	}
	assert.Empty(suite.T(), frc.deleted)
}

type fakeRetentionClient struct {
	deleted []string
}

// GetCandidates ...
func (frc *fakeRetentionClient) GetCandidates(repo *selector.Repository) ([]*selector.Candidate, error) {
	return nil, errors.New("not implemented")
}

// Delete ...
func (frc *fakeRetentionClient) Delete(candidate *selector.Candidate) error {
	frc.deleted = append(frc.deleted, candidate.Digest)
	return nil
}

// DeleteRepository ...
func (frc *fakeRetentionClient) DeleteRepository(repo *selector.Repository) error {
	panic("implement me")
}
