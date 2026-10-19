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

	"github.com/goharbor/harbor/src/controller/event/metadata"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/selector"
	"github.com/goharbor/harbor/src/pkg/notifier/event"
	"github.com/goharbor/harbor/src/pkg/task"
)

func init() {
	if err := task.RegisterCheckInProcessor(job.RetentionVendorType, retentionTaskCheckInProcessor); err != nil {
		log.Fatalf("failed to register the checkin processor for the retention job, error %v", err)
	}
}

func retentionTaskCheckInProcessor(ctx context.Context, t *task.Task, sc *job.StatusChange) (err error) {
	taskID := t.ID
	status := t.Status
	log.Debugf("received retention task status update event: task-%d, status-%s", taskID, status)
	// handle checkin
	if sc.CheckIn != "" {
		var retainObj struct {
			Total                     int                `json:"total"`
			Retained                  int                `json:"retained"`
			Deleted                   []*selector.Result `json:"deleted"`
			DeletionCandidates        int                `json:"deletion_candidates"`
			MaxDeletionsPerRepository *int               `json:"max_deletions_per_repository,omitempty"`
			DeletionBlocked           bool               `json:"deletion_blocked"`
		}
		if err := json.Unmarshal([]byte(sc.CheckIn), &retainObj); err != nil {
			log.Errorf("failed to resolve checkin of retention task %d: %v", taskID, err)

			return err
		}

		t, err := task.Mgr.Get(ctx, taskID)
		if err != nil {
			return err
		}

		t.ExtraAttrs["total"] = retainObj.Total
		t.ExtraAttrs["retained"] = retainObj.Retained
		t.ExtraAttrs["deletion_candidates"] = retainObj.DeletionCandidates
		t.ExtraAttrs["deletion_blocked"] = retainObj.DeletionBlocked
		// only record the threshold when it is configured, so absence keeps
		// the same behavior as the old checkin data
		if retainObj.MaxDeletionsPerRepository != nil {
			t.ExtraAttrs["max_deletions_per_repository"] = *retainObj.MaxDeletionsPerRepository
		} else {
			delete(t.ExtraAttrs, "max_deletions_per_repository")
		}

		err = task.Mgr.UpdateExtraAttrs(ctx, taskID, t.ExtraAttrs)
		if err != nil {
			log.G(ctx).WithField("error", err).Errorf("failed to update of retention task %d", taskID)
			return err
		}

		e := &event.Event{}
		metaData := &metadata.RetentionMetaData{
			Total:    retainObj.Total,
			Retained: retainObj.Retained,
			Deleted:  retainObj.Deleted,
			Status:   "SUCCESS",
			TaskID:   taskID,
		}

		if err := e.Build(ctx, metaData); err == nil {
			if err := e.Publish(ctx); err != nil {
				log.G(ctx).WithField("error", err).Errorf("tag retention job hook handler: event publish")
			}
		} else {
			log.G(ctx).WithField("error", err).Errorf("tag retention job hook handler: event publish")
		}
	}
	return nil
}
