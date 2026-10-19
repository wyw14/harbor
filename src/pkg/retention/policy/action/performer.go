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
	"context"

	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/selector"
	"github.com/goharbor/harbor/src/pkg/immutable/match/rule"
	"github.com/goharbor/harbor/src/pkg/retention/dep"
)

const (
	// Retain artifacts
	Retain = "retain"
)

// Performer performs the related actions targeting the candidates
type Performer interface {
	// Perform the action
	//
	//  Arguments:
	//    candidates []*art.Candidate : the targets to perform
	//
	//  Returns:
	//    []*art.Result : result infos
	//    error     : common error if any errors occurred
	Perform(ctx context.Context, candidates []*selector.Candidate) ([]*selector.Result, error)
}

// PerformerFactory is factory method for creating Performer
type PerformerFactory func(params any, isDryRun bool, maxDeletionsPerRepository *int) Performer

// retainAction make sure all the candidates will be retained and others will be cleared
type retainAction struct {
	all []*selector.Candidate
	// Indicate if it is a dry run
	isDryRun bool
	// maxDeletionsPerRepository is the per-repository deletion protection
	// threshold, nil means no threshold
	maxDeletionsPerRepository *int
}

// Perform the action
func (ra *retainAction) Perform(ctx context.Context, candidates []*selector.Candidate) (results []*selector.Result, err error) {
	retainedShare := make(map[string]bool)
	immutableShare := make(map[string]bool)
	for _, c := range candidates {
		retainedShare[c.Hash()] = true
	}

	// deletionCandidates are the artifacts not retained by the rules and
	// not protected by immutable tags, they are the same candidate set used
	// for both the threshold check and the deletion.
	deletionCandidates := make([]*selector.Candidate, 0)
	for _, c := range ra.all {
		if _, ok := retainedShare[c.Hash()]; ok {
			continue
		}
		if isImmutable(ctx, c) {
			immutableShare[c.Hash()] = true
		}
	}
	for _, c := range ra.all {
		if _, ok := retainedShare[c.Hash()]; ok {
			continue
		}
		if _, ok := immutableShare[c.Hash()]; !ok {
			deletionCandidates = append(deletionCandidates, c)
		}
	}

	// count by unique artifact digest, multiple tags of one artifact count once
	candidateDigests := make(map[string]struct{}, len(deletionCandidates))
	for _, c := range deletionCandidates {
		candidateDigests[c.Digest] = struct{}{}
	}
	candidateCount := len(candidateDigests)
	// all-or-nothing: when the count is greater than the threshold, the whole
	// repository keeps every artifact in this round.
	blocked := ra.maxDeletionsPerRepository != nil && candidateCount > *ra.maxDeletionsPerRepository
	if blocked {
		log.Warningf("retention skipped for repository %s/%s: %d artifact(s) selected for deletion exceeds max_deletions_per_repository %d",
			deletionCandidates[0].Namespace, deletionCandidates[0].Repository, candidateCount, *ra.maxDeletionsPerRepository)
	}

	// start to delete
	if len(ra.all) > 0 {
		for _, c := range ra.all {
			if _, ok := retainedShare[c.Hash()]; !ok {
				result := &selector.Result{
					Target: c,
				}
				if _, ok = immutableShare[c.Hash()]; ok {
					result.Error = &selector.ImmutableError{}
				} else if blocked {
					result.Error = &selector.MaxDeletionsExceededError{
						CandidateCount: candidateCount,
						Max:            *ra.maxDeletionsPerRepository,
					}
				} else {
					if !ra.isDryRun {
						if err := dep.DefaultClient.Delete(c); err != nil {
							result.Error = err
						}
					}
				}
				results = append(results, result)
			}
		}
	}

	return
}

func isImmutable(ctx context.Context, c *selector.Candidate) bool {
	projectID := c.NamespaceID
	matched, err := rule.NewRuleMatcher().Match(ctx, projectID, selector.Candidate{
		Repository:  c.Repository,
		Tags:        c.Tags,
		NamespaceID: projectID,
	})
	if err != nil {
		log.Error(err)
		return false
	}
	return matched
}

// NewRetainAction is factory method for RetainAction
func NewRetainAction(params any, isDryRun bool, maxDeletionsPerRepository *int) Performer {
	all := make([]*selector.Candidate, 0)
	if params != nil {
		if a, ok := params.([]*selector.Candidate); ok {
			all = a
		}
	}

	return &retainAction{
		all:                       all,
		isDryRun:                  isDryRun,
		maxDeletionsPerRepository: maxDeletionsPerRepository,
	}
}
