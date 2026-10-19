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

package artifact

import (
	"context"

	"github.com/opencontainers/go-digest"

	"github.com/goharbor/harbor/src/common/rbac"
	rbac_project "github.com/goharbor/harbor/src/common/rbac/project"
	"github.com/goharbor/harbor/src/common/security"
	"github.com/goharbor/harbor/src/common/utils"
	"github.com/goharbor/harbor/src/controller/artifact/manifest"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/q"
	"github.com/goharbor/harbor/src/pkg/artifact"
	scanv1 "github.com/goharbor/harbor/src/pkg/scan/rest/v1"
)

// DiffTarget identifies one side of an artifact metadata diff by repository
// and digest. The digest is the content identity; the repository determines
// provenance (tags, signatures and scan summaries are per repository).
type DiffTarget struct {
	Repository string
	Digest     string
}

// DiffOption controls an artifact metadata diff query.
type DiffOption struct {
	// WithScanSummary includes the vulnerability scan summary in the comparison.
	// The summary is read-only: the query never triggers a scan.
	WithScanSummary bool
}

// ScanSummaryProvider is the read-only slice of the scan controller used by
// Diff. controller/scan already imports this package, so the real controller
// registers itself through RegisterScanSummaryProvider to avoid an import
// cycle. It deliberately exposes no Scan method, making it impossible for the
// diff query to trigger a scan.
type ScanSummaryProvider interface {
	GetSummary(ctx context.Context, art *Artifact, scanType string, mimeTypes []string) (map[string]any, error)
}

var scanSummaryProvider ScanSummaryProvider

// RegisterScanSummaryProvider wires the read-only scan summary lookup. It is
// invoked from controller/scan, whose default controller satisfies the
// interface.
func RegisterScanSummaryProvider(provider ScanSummaryProvider) {
	scanSummaryProvider = provider
}

// Diff compares the read-only metadata of two artifacts identified by digest.
// It compares tags, declared platforms, signature references, scan summaries
// (when requested) and the creation time, and marks sub-manifests an index
// declares but one side cannot serve. The call never triggers a scan and
// never creates, moves or deletes tags: it only reads the database and the
// backend registry. Read permission is required on both projects, so artifacts
// in projects the caller cannot see are not compared.
func (c *controller) Diff(ctx context.Context, left, right *DiffTarget, option *DiffOption, query *q.Query) (*artifact.DiffResult, error) {
	if left == nil || right == nil {
		return nil, errors.New(nil).WithCode(errors.BadRequestCode).
			WithMessage("both sides of the diff must be specified")
	}
	if err := validateDiffTarget(left); err != nil {
		return nil, err
	}
	if err := validateDiffTarget(right); err != nil {
		return nil, err
	}
	// authorize before any data access: the caller needs read on both projects,
	// including when the artifacts live in different projects.
	if err := c.authorizeDiffRead(ctx, left.Repository); err != nil {
		return nil, err
	}
	if err := c.authorizeDiffRead(ctx, right.Repository); err != nil {
		return nil, err
	}

	if option == nil {
		option = &DiffOption{}
	}
	leftSnapshot, err := c.buildDiffSnapshot(ctx, left, option)
	if err != nil {
		return nil, err
	}
	rightSnapshot, err := c.buildDiffSnapshot(ctx, right, option)
	if err != nil {
		return nil, err
	}
	return artifact.Compare(leftSnapshot, rightSnapshot, query), nil
}

func validateDiffTarget(target *DiffTarget) error {
	if target.Repository == "" {
		return errors.New(nil).WithCode(errors.BadRequestCode).WithMessage("repository must be specified")
	}
	if target.Digest == "" {
		return errors.New(nil).WithCode(errors.BadRequestCode).WithMessage("digest must be specified")
	}
	if _, err := digest.Parse(target.Digest); err != nil {
		return errors.New(nil).WithCode(errors.BadRequestCode).
			WithMessagef("invalid digest %q: %v", target.Digest, err)
	}
	return nil
}

// authorizeDiffRead enforces read permission on the artifact resource of the
// project owning the repository, for each diff side independently.
func (c *controller) authorizeDiffRead(ctx context.Context, repository string) error {
	projectName, _ := utils.ParseRepository(repository)
	securityCtx, ok := security.FromContext(ctx)
	if !ok {
		return errors.UnauthorizedError(errors.New("security context not found"))
	}
	project, err := c.proCtl.GetByName(ctx, projectName)
	if err != nil {
		return err
	}
	resource := rbac_project.NewNamespace(project.ProjectID).Resource(rbac.ResourceArtifact)
	if securityCtx.Can(ctx, rbac.ActionRead, resource) {
		return nil
	}
	if !securityCtx.IsAuthenticated() {
		return errors.UnauthorizedError(nil)
	}
	return errors.ForbiddenError(nil).
		WithMessagef("no read permission for the artifact in project %s", projectName)
}

// buildDiffSnapshot assembles the metadata snapshot of one side using only
// read-only operations: artifact/tag/accessory list lookups, one registry
// manifest pull and manifest existence checks for declared children. It never
// calls Ensure, never updates pull time and never triggers a scan.
func (c *controller) buildDiffSnapshot(ctx context.Context, target *DiffTarget, option *DiffOption) (*artifact.Snapshot, error) {
	art, err := c.artMgr.GetByDigest(ctx, target.Repository, target.Digest)
	if err != nil {
		return nil, err
	}
	projectName, _ := utils.ParseRepository(target.Repository)
	snapshot := &artifact.Snapshot{
		Source: artifact.Source{
			ProjectName:    projectName,
			RepositoryName: target.Repository,
			Digest:         target.Digest,
		},
		ManifestMediaType: art.ManifestMediaType,
		CreatedAt:         art.PushTime,
	}

	// tags: list only, no Ensure/Create/Update
	tags, err := c.tagCtl.List(ctx, &q.Query{
		Keywords: map[string]any{"artifact_id": art.ID},
	}, nil)
	if err != nil {
		return nil, err
	}
	for _, t := range tags {
		snapshot.Tags = append(snapshot.Tags, t.Name)
	}

	// signature accessories referencing this artifact
	accessories, err := c.accessoryMgr.List(ctx, q.New(q.KeyWords{"SubjectArtifactID": art.ID}))
	if err != nil {
		return nil, err
	}
	for _, acc := range accessories {
		if !isSignature(acc) {
			continue
		}
		data := acc.GetData()
		snapshot.Signatures = append(snapshot.Signatures, artifact.SignatureRef{
			Digest: data.Digest,
			Type:   data.Type,
		})
	}

	// read the raw manifest from the registry and let the manifest adapter layer
	// describe the children it declares, without requiring Harbor metadata
	if err := c.populateDiffChildren(ctx, target.Repository, snapshot); err != nil {
		return nil, err
	}

	if option.WithScanSummary && scanSummaryProvider != nil {
		view := &Artifact{Artifact: *art}
		summary, sumErr := scanSummaryProvider.GetSummary(ctx, view,
			scanv1.ScanTypeVulnerability, []string{scanv1.MimeTypeNativeReport})
		if sumErr != nil {
			// the diff stays usable when the scan summary is unavailable on one side:
			// log and compare without it instead of failing the whole query
			log.G(ctx).Warningf("failed to get the scan summary for %s@%s: %v",
				target.Repository, target.Digest, sumErr)
		} else {
			snapshot.ScanSummary = summary
		}
	}

	return snapshot, nil
}

// populateDiffChildren pulls the manifest read-only, parses its declared
// children through the manifest adapters and flags those missing from the
// registry or Harbor's artifact metadata.
func (c *controller) populateDiffChildren(ctx context.Context, repository string, snapshot *artifact.Snapshot) error {
	mf, _, err := c.regCli.PullManifest(repository, snapshot.Source.Digest)
	if err != nil {
		return err
	}
	mediaType, content, err := mf.Payload()
	if err != nil {
		return err
	}
	snapshot.ManifestMediaType = mediaType
	descriptors, err := manifest.PeekReferences(mediaType, content)
	if err != nil {
		return err
	}
	for _, descriptor := range descriptors {
		child := artifact.ChildManifest{
			Digest:      descriptor.Digest,
			MediaType:   descriptor.MediaType,
			Platform:    descriptor.Platform,
			URLs:        descriptor.URLs,
			Annotations: descriptor.Annotations,
		}
		missing, reason, err := c.inspectDiffChild(ctx, repository, descriptor.Digest)
		if err != nil {
			return err
		}
		child.Missing = missing
		child.Reason = reason
		snapshot.Children = append(snapshot.Children, child)

		if descriptor.Platform != nil {
			snapshot.Platforms = append(snapshot.Platforms, artifact.PlatformKey(descriptor.Platform))
		}
	}
	return nil
}

// inspectDiffChild reports a declared child manifest as missing. The backend
// registry is authoritative: absence there is MissingInRegistry even if a
// stale metadata row exists. Otherwise a missing Harbor record is
// MissingInDatabase. Lookup errors other than "not found" are returned so
// infrastructure failures are not mistaken for a missing manifest.
func (c *controller) inspectDiffChild(ctx context.Context, repository, digest string) (bool, string, error) {
	exist, _, err := c.regCli.ManifestExist(repository, digest)
	if err != nil {
		return false, "", err
	}
	if !exist {
		return true, artifact.MissingInRegistry, nil
	}
	_, err = c.artMgr.GetByDigest(ctx, repository, digest)
	if err != nil {
		if errors.IsErr(err, errors.NotFoundCode) {
			return true, artifact.MissingInDatabase, nil
		}
		return false, "", err
	}
	return false, "", nil
}
