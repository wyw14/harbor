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

package selector

import "fmt"

// Result keeps the action result
type Result struct {
	Target *Candidate `json:"target"`
	// nil error means success
	Error error `json:"error"`
}

// ImmutableError ...
type ImmutableError struct {
}

func (e *ImmutableError) Error() string {
	return "Immutable tag"
}

// MaxDeletionsExceededError indicates that the target is retained in this
// round because the deletion candidate count of the repository is greater
// than the max_deletions_per_repository protection threshold, so no
// artifact of the repository is deleted.
type MaxDeletionsExceededError struct {
	// CandidateCount is the unique artifact digest count of the deletion
	// candidates for the repository in this round
	CandidateCount int
	// Max is the applied max_deletions_per_repository threshold
	Max int
}

func (e *MaxDeletionsExceededError) Error() string {
	return fmt.Sprintf("deletion skipped: %d artifact(s) selected for deletion exceeds the max_deletions_per_repository threshold %d", e.CandidateCount, e.Max)
}
