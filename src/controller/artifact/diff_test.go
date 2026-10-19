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
	"testing"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/common/security"
	"github.com/goharbor/harbor/src/controller/tag"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/q"
	accessorymodel "github.com/goharbor/harbor/src/pkg/accessory/model"
	basemodel "github.com/goharbor/harbor/src/pkg/accessory/model/base"
	"github.com/goharbor/harbor/src/pkg/artifact"
	projectModel "github.com/goharbor/harbor/src/pkg/project/models"
	scanv1 "github.com/goharbor/harbor/src/pkg/scan/rest/v1"
	model_tag "github.com/goharbor/harbor/src/pkg/tag/model/tag"
	sectesting "github.com/goharbor/harbor/src/testing/common/security"
	projecttesting "github.com/goharbor/harbor/src/testing/controller/project"
	tagtesting "github.com/goharbor/harbor/src/testing/controller/tag"
	accessorytesting "github.com/goharbor/harbor/src/testing/pkg/accessory"
	arttesting "github.com/goharbor/harbor/src/testing/pkg/artifact"
	disttesting "github.com/goharbor/harbor/src/testing/pkg/distribution"
	regtesting "github.com/goharbor/harbor/src/testing/pkg/registry"
)

const (
	diffIndexDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	diffAmdDigest   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	diffArmDigest   = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	diffPpcDigest   = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	diffSigDigest   = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
)

const (
	leftIndexContent = `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.index.v1+json",
   "manifests": [
      {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 10,
       "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
       "platform": {"architecture": "amd64", "os": "linux"}},
      {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 10,
       "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
       "platform": {"architecture": "arm64", "os": "linux", "variant": "v8"}}
   ]
}`
	rightIndexContent = `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.index.v1+json",
   "manifests": [
      {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 10,
       "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
       "platform": {"architecture": "amd64", "os": "linux"}},
      {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 10,
       "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
       "platform": {"architecture": "arm64", "os": "linux", "variant": "v8"}},
      {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 10,
       "digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333",
       "platform": {"architecture": "ppc64le", "os": "linux"}}
   ]
}`
	emptyIndexContent = `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.index.v1+json",
   "manifests": []
}`
)

// fakeScanSummaryProvider only exposes the read-only GetSummary lookup, so the
// diff code path cannot trigger a scan even accidentally.
type fakeScanSummaryProvider struct {
	mock.Mock
}

func (f *fakeScanSummaryProvider) GetSummary(ctx context.Context, art *Artifact, scanType string, mimeTypes []string) (map[string]any, error) {
	args := f.Called(ctx, art, scanType, mimeTypes)
	var summary map[string]any
	if args.Get(0) != nil {
		summary = args.Get(0).(map[string]any)
	}
	return summary, args.Error(1)
}

type diffControllerTestSuite struct {
	suite.Suite
	ctl      *controller
	artMgr   *arttesting.Manager
	tagCtl   *tagtesting.FakeController
	accMgr   *accessorytesting.Manager
	regCli   *regtesting.Client
	proCtl   *projecttesting.Controller
	secCtx   *sectesting.Context
	scanProv *fakeScanSummaryProvider
}

func (d *diffControllerTestSuite) SetupTest() {
	d.artMgr = &arttesting.Manager{}
	d.tagCtl = &tagtesting.FakeController{}
	d.accMgr = &accessorytesting.Manager{}
	d.regCli = &regtesting.Client{}
	d.proCtl = &projecttesting.Controller{}
	d.secCtx = &sectesting.Context{}
	d.scanProv = &fakeScanSummaryProvider{}
	d.ctl = &controller{
		artMgr:       d.artMgr,
		tagCtl:       d.tagCtl,
		accessoryMgr: d.accMgr,
		regCli:       d.regCli,
		proCtl:       d.proCtl,
	}
	scanSummaryProvider = d.scanProv
	d.T().Cleanup(func() { scanSummaryProvider = nil })
}

func (d *diffControllerTestSuite) securityContext() context.Context {
	return security.NewContext(context.Background(), d.secCtx)
}

func (d *diffControllerTestSuite) allowProjectReads() {
	d.proCtl.On("GetByName", mock.Anything, "library").
		Return(&projectModel.Project{ProjectID: 1, Name: "library"}, nil)
	d.proCtl.On("GetByName", mock.Anything, "docker").
		Return(&projectModel.Project{ProjectID: 2, Name: "docker"}, nil)
	d.secCtx.On("Can", mock.Anything, mock.Anything, mock.Anything).Return(true)
	d.secCtx.On("IsAuthenticated").Return(true)
}

func diffTag(name string) *tag.Tag {
	return &tag.Tag{Tag: model_tag.Tag{Name: name}}
}

// TestDiffMultiArchIndex covers the whole flow across two projects: different
// tags, an extra platform on the right, a signature only on the left,
// different scan summaries and creation times, plus two flavors of missing
// sub-manifests. The arm64 child is missing from Harbor metadata on the left
// but absent from the backend registry on the right.
func (d *diffControllerTestSuite) TestDiffMultiArchIndex() {
	d.allowProjectReads()

	// root index records on both sides
	d.artMgr.On("GetByDigest", mock.Anything, "library/app", diffIndexDigest).
		Return(&artifact.Artifact{
			ID: 10, ProjectID: 1, RepositoryName: "library/app", Digest: diffIndexDigest,
			ManifestMediaType: v1.MediaTypeImageIndex,
			PushTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}, nil)
	d.artMgr.On("GetByDigest", mock.Anything, "docker/app", diffIndexDigest).
		Return(&artifact.Artifact{
			ID: 20, ProjectID: 2, RepositoryName: "docker/app", Digest: diffIndexDigest,
			ManifestMediaType: v1.MediaTypeImageIndex,
			PushTime:          time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		}, nil)
	// child metadata lookups: arm64 missing in database on the left, all others present
	d.artMgr.On("GetByDigest", mock.Anything, "library/app", diffArmDigest).
		Return(nil, errors.New(nil).WithCode(errors.NotFoundCode).WithMessage("not found"))
	d.artMgr.On("GetByDigest", mock.Anything, "library/app", diffAmdDigest).
		Return(&artifact.Artifact{ID: 11, RepositoryName: "library/app", Digest: diffAmdDigest}, nil)
	d.artMgr.On("GetByDigest", mock.Anything, "docker/app", diffAmdDigest).
		Return(&artifact.Artifact{ID: 21, RepositoryName: "docker/app", Digest: diffAmdDigest}, nil)
	d.artMgr.On("GetByDigest", mock.Anything, "docker/app", diffPpcDigest).
		Return(&artifact.Artifact{ID: 22, RepositoryName: "docker/app", Digest: diffPpcDigest}, nil)

	// tag lists (read-only), left snapshot is built first
	d.tagCtl.On("List").Return([]*tag.Tag{diffTag("v1"), diffTag("latest")}, nil).Once()
	d.tagCtl.On("List").Return([]*tag.Tag{diffTag("v2"), diffTag("latest")}, nil).Once()

	// signature accessories: one cosign signature on the left, none on the right
	signature := &basemodel.Default{
		Data: accessorymodel.AccessoryData{
			ID:                30,
			ArtifactID:        10,
			SubArtifactDigest: diffIndexDigest,
			Digest:            diffSigDigest,
			Type:              accessorymodel.TypeCosignSignature,
		},
	}
	d.accMgr.On("List", mock.Anything, mock.Anything).
		Return([]accessorymodel.Accessory{signature}, nil).Once()
	d.accMgr.On("List", mock.Anything, mock.Anything).
		Return([]accessorymodel.Accessory{}, nil).Once()

	// raw manifests pulled read-only from the registry
	leftMF := &disttesting.Manifest{}
	leftMF.On("Payload").Return(v1.MediaTypeImageIndex, []byte(leftIndexContent), nil)
	rightMF := &disttesting.Manifest{}
	rightMF.On("Payload").Return(v1.MediaTypeImageIndex, []byte(rightIndexContent), nil)
	d.regCli.On("PullManifest", "library/app", diffIndexDigest).Return(leftMF, diffIndexDigest, nil)
	d.regCli.On("PullManifest", "docker/app", diffIndexDigest).Return(rightMF, diffIndexDigest, nil)

	// registry existence: left arm64 present in registry but absent in metadata;
	// right arm64 absent from the registry itself
	d.regCli.On("ManifestExist", "library/app", diffAmdDigest).Return(true, nil, nil)
	d.regCli.On("ManifestExist", "library/app", diffArmDigest).Return(true, nil, nil)
	d.regCli.On("ManifestExist", "docker/app", diffAmdDigest).Return(true, nil, nil)
	d.regCli.On("ManifestExist", "docker/app", diffArmDigest).Return(false, nil, nil)
	d.regCli.On("ManifestExist", "docker/app", diffPpcDigest).Return(true, nil, nil)

	// scan summaries are fetched read-only and never trigger a scan
	d.scanProv.On("GetSummary", mock.Anything,
		mock.MatchedBy(func(a *Artifact) bool { return a.RepositoryName == "library/app" }),
		scanv1.ScanTypeVulnerability, []string{scanv1.MimeTypeNativeReport}).
		Return(map[string]any{"severity": "High", "scan_status": "Success"}, nil)
	d.scanProv.On("GetSummary", mock.Anything,
		mock.MatchedBy(func(a *Artifact) bool { return a.RepositoryName == "docker/app" }),
		scanv1.ScanTypeVulnerability, []string{scanv1.MimeTypeNativeReport}).
		Return(map[string]any{"severity": "Low", "scan_status": "Success"}, nil)

	result, err := d.ctl.Diff(d.securityContext(),
		&DiffTarget{Repository: "library/app", Digest: diffIndexDigest},
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest},
		&DiffOption{WithScanSummary: true}, nil)
	d.Require().NoError(err)

	// tags with provenance
	tags := d.entriesByField(result, artifact.DiffFieldTags)
	d.ElementsMatch([]string{"v1", "v2"}, d.entryPaths(tags))
	v1Entry := d.findEntry(result, artifact.DiffFieldTags, "v1")
	d.Require().NotNil(v1Entry)
	d.Equal(artifact.DiffSideLeft, v1Entry.Side)
	d.Equal("library", v1Entry.Source.ProjectName)
	v2Entry := d.findEntry(result, artifact.DiffFieldTags, "v2")
	d.Require().NotNil(v2Entry)
	d.Equal(artifact.DiffSideRight, v2Entry.Side)
	d.Equal("docker", v2Entry.Source.ProjectName)

	// the shared tag and the equal scan status are not reported
	d.Nil(d.findEntry(result, artifact.DiffFieldTags, "latest"))
	d.Nil(d.findEntry(result, artifact.DiffFieldScanSummary, "scan_summary.scan_status"))

	// platform only present on the right
	platformEntry := d.findEntry(result, artifact.DiffFieldPlatforms, "linux/ppc64le")
	d.Require().NotNil(platformEntry)
	d.Equal(artifact.DiffSideRight, platformEntry.Side)
	d.Equal("docker/app", platformEntry.Source.RepositoryName)
	d.Nil(d.findEntry(result, artifact.DiffFieldPlatforms, "linux/amd64"))
	d.Nil(d.findEntry(result, artifact.DiffFieldPlatforms, "linux/arm64/v8"),
		"the arm64 platform exists on both indexes; its absence is reported as a missing child, not a platform diff")

	// signature reference only on the left
	sigEntry := d.findEntry(result, artifact.DiffFieldSignatures, diffSigDigest)
	d.Require().NotNil(sigEntry)
	d.Equal(artifact.DiffSideLeft, sigEntry.Side)
	d.Equal(accessorymodel.TypeCosignSignature, sigEntry.Left.(artifact.SignatureRef).Type)

	// scan summary differing key with both values
	severity := d.findEntry(result, artifact.DiffFieldScanSummary, "scan_summary.severity")
	d.Require().NotNil(severity)
	d.Equal("High", severity.Left)
	d.Equal("Low", severity.Right)

	// creation time differs
	createdAt := d.findEntry(result, artifact.DiffFieldCreatedAt, "created_at")
	d.Require().NotNil(createdAt)
	d.Equal("2026-01-01T00:00:00Z", createdAt.Left)
	d.Equal("2026-02-01T00:00:00Z", createdAt.Right)

	// missing sub-manifests marked separately, one per side with distinct reason
	d.Require().Len(result.MissingChildren, 2)
	d.Equal(artifact.DiffSideLeft, result.MissingChildren[0].Side)
	d.Equal("library/app", result.MissingChildren[0].Source.RepositoryName)
	d.Equal(diffArmDigest, result.MissingChildren[0].Child.Digest)
	d.True(result.MissingChildren[0].Child.Missing)
	d.Equal(artifact.MissingInDatabase, result.MissingChildren[0].Child.Reason)
	d.Equal(artifact.DiffSideRight, result.MissingChildren[1].Side)
	d.Equal(artifact.MissingInRegistry, result.MissingChildren[1].Child.Reason)

	// the right arm64 child was reported missing straight from the registry,
	// without querying its metadata
	d.artMgr.AssertNotCalled(d.T(), "GetByDigest", mock.Anything, "docker/app", diffArmDigest)
}

// TestDiffIsReadOnly asserts the query neither mutates tags nor triggers a
// scan: the scan provider interface exposes no trigger, and the tag
// controller write methods are never invoked.
func (d *diffControllerTestSuite) TestDiffIsReadOnly() {
	d.allowProjectReads()

	d.artMgr.On("GetByDigest", mock.Anything, "library/app", diffIndexDigest).
		Return(&artifact.Artifact{ID: 10, RepositoryName: "library/app", Digest: diffIndexDigest,
			PushTime: time.Now()}, nil)
	d.artMgr.On("GetByDigest", mock.Anything, "docker/app", diffIndexDigest).
		Return(&artifact.Artifact{ID: 20, RepositoryName: "docker/app", Digest: diffIndexDigest,
			PushTime: time.Now()}, nil)
	d.tagCtl.On("List").Return([]*tag.Tag{}, nil).Twice()
	d.accMgr.On("List", mock.Anything, mock.Anything).Return([]accessorymodel.Accessory{}, nil).Twice()

	mf := &disttesting.Manifest{}
	mf.On("Payload").Return(v1.MediaTypeImageIndex, []byte(emptyIndexContent), nil)
	d.regCli.On("PullManifest", mock.Anything, mock.Anything).Return(mf, diffIndexDigest, nil)

	// scan summaries must not be fetched when the option is off
	_, err := d.ctl.Diff(d.securityContext(),
		&DiffTarget{Repository: "library/app", Digest: diffIndexDigest},
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest},
		&DiffOption{WithScanSummary: false}, nil)
	d.Require().NoError(err)

	d.scanProv.AssertNotCalled(d.T(), "GetSummary", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	d.tagCtl.AssertNotCalled(d.T(), "Ensure")
	d.tagCtl.AssertNotCalled(d.T(), "Create")
	d.tagCtl.AssertNotCalled(d.T(), "Update")
	d.regCli.AssertNotCalled(d.T(), "PushManifest", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	d.regCli.AssertNotCalled(d.T(), "DeleteManifest", mock.Anything, mock.Anything)
}

// TestDiffCrossProjectForbidden denies the right project: read access on both
// projects is mandatory and no artifact data may be touched on denial.
func (d *diffControllerTestSuite) TestDiffCrossProjectForbidden() {
	d.proCtl.On("GetByName", mock.Anything, "library").
		Return(&projectModel.Project{ProjectID: 1, Name: "library"}, nil)
	d.proCtl.On("GetByName", mock.Anything, "docker").
		Return(&projectModel.Project{ProjectID: 2, Name: "docker"}, nil)
	d.secCtx.On("Can", mock.Anything, mock.Anything, mock.Anything).Return(true).Once()
	d.secCtx.On("Can", mock.Anything, mock.Anything, mock.Anything).Return(false).Once()
	d.secCtx.On("IsAuthenticated").Return(true)

	_, err := d.ctl.Diff(d.securityContext(),
		&DiffTarget{Repository: "library/app", Digest: diffIndexDigest},
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest}, nil, nil)
	d.Require().Error(err)
	d.Equal(errors.ForbiddenCode, errors.ErrCode(err))
	d.artMgr.AssertNotCalled(d.T(), "GetByDigest", mock.Anything, mock.Anything, mock.Anything)
	d.regCli.AssertNotCalled(d.T(), "PullManifest", mock.Anything, mock.Anything)
}

// An unauthenticated caller without read on the second project gets
// unauthorized rather than forbidden.
func (d *diffControllerTestSuite) TestDiffUnauthenticated() {
	d.proCtl.On("GetByName", mock.Anything, mock.Anything).
		Return(&projectModel.Project{ProjectID: 1}, nil)
	d.secCtx.On("Can", mock.Anything, mock.Anything, mock.Anything).Return(false)
	d.secCtx.On("IsAuthenticated").Return(false)

	_, err := d.ctl.Diff(d.securityContext(),
		&DiffTarget{Repository: "library/app", Digest: diffIndexDigest},
		&DiffTarget{Repository: "library/b", Digest: diffIndexDigest}, nil, nil)
	d.Require().Error(err)
	d.Equal(errors.UnAuthorizedCode, errors.ErrCode(err))
}

// Without any security context the diff cannot authorize either side.
func (d *diffControllerTestSuite) TestDiffMissingSecurityContext() {
	_, err := d.ctl.Diff(context.Background(),
		&DiffTarget{Repository: "library/app", Digest: diffIndexDigest},
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest}, nil, nil)
	d.Require().Error(err)
	d.Equal(errors.UnAuthorizedCode, errors.ErrCode(err))
}

func (d *diffControllerTestSuite) TestDiffInvalidTargets() {
	d.allowProjectReads()

	// malformed digest is rejected as a bad request without touching managers
	_, err := d.ctl.Diff(d.securityContext(),
		&DiffTarget{Repository: "library/app", Digest: "not-a-digest"},
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest}, nil, nil)
	d.Require().Error(err)
	d.Equal(errors.BadRequestCode, errors.ErrCode(err))

	_, err = d.ctl.Diff(d.securityContext(), nil,
		&DiffTarget{Repository: "docker/app", Digest: diffIndexDigest}, nil, nil)
	d.Require().Error(err)
	d.Equal(errors.BadRequestCode, errors.ErrCode(err))

	d.artMgr.AssertNotCalled(d.T(), "GetByDigest", mock.Anything, mock.Anything, mock.Anything)
}

// TestDiffPaginationAndSorting exercises paging of tag differences across
// projects: total is stable, pages are ordered by path and scan summaries stay
// untouched.
func (d *diffControllerTestSuite) TestDiffPaginationAndSorting() {
	d.proCtl.On("GetByName", mock.Anything, "library").
		Return(&projectModel.Project{ProjectID: 1, Name: "library"}, nil)
	d.proCtl.On("GetByName", mock.Anything, "docker").
		Return(&projectModel.Project{ProjectID: 2, Name: "docker"}, nil)
	d.secCtx.On("Can", mock.Anything, mock.Anything, mock.Anything).Return(true)

	// every Diff rebuilds both snapshots read-only; snapshots are assembled in
	// left/right order, so expectations alternate per Diff call
	leftTags := []*tag.Tag{diffTag("e"), diffTag("c"), diffTag("a"), diffTag("d"), diffTag("b")}
	d.artMgr.On("GetByDigest", mock.Anything, "library/many", diffIndexDigest).
		Return(&artifact.Artifact{ID: 10, RepositoryName: "library/many", Digest: diffIndexDigest,
			PushTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, nil)
	d.artMgr.On("GetByDigest", mock.Anything, "docker/many", diffIndexDigest).
		Return(&artifact.Artifact{ID: 20, RepositoryName: "docker/many", Digest: diffIndexDigest,
			PushTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, nil)
	// two Diff calls below -> four snapshot assemblies (left, right, left, right)
	d.tagCtl.On("List").Return(leftTags, nil).Once()
	d.tagCtl.On("List").Return([]*tag.Tag{}, nil).Once()
	d.tagCtl.On("List").Return(leftTags, nil).Once()
	d.tagCtl.On("List").Return([]*tag.Tag{}, nil).Once()
	d.accMgr.On("List", mock.Anything, mock.Anything).Return([]accessorymodel.Accessory{}, nil).Times(4)

	leftMF := &disttesting.Manifest{}
	leftMF.On("Payload").Return(v1.MediaTypeImageIndex, []byte(emptyIndexContent), nil)
	rightMF := &disttesting.Manifest{}
	rightMF.On("Payload").Return(v1.MediaTypeImageIndex, []byte(emptyIndexContent), nil)
	d.regCli.On("PullManifest", "library/many", diffIndexDigest).Return(leftMF, diffIndexDigest, nil)
	d.regCli.On("PullManifest", "docker/many", diffIndexDigest).Return(rightMF, diffIndexDigest, nil)

	left := &DiffTarget{Repository: "library/many", Digest: diffIndexDigest}
	right := &DiffTarget{Repository: "docker/many", Digest: diffIndexDigest}

	// first page with a stable total over all five tag differences
	page1, err := d.ctl.Diff(d.securityContext(), left, right, nil,
		&q.Query{PageNumber: 1, PageSize: 2})
	d.Require().NoError(err)
	d.Equal(int64(5), page1.Total)
	d.Equal([]string{"a", "b"}, d.entryPaths(page1.Entries))

	// no page size on the second Diff: all entries in path order
	all, err := d.ctl.Diff(d.securityContext(), left, right, nil, &q.Query{})
	d.Require().NoError(err)
	d.Equal(int64(5), all.Total)
	d.Equal([]string{"a", "b", "c", "d", "e"}, d.entryPaths(all.Entries))
	d.Empty(all.MissingChildren)
}

func (d *diffControllerTestSuite) findEntry(result *artifact.DiffResult, field artifact.DiffField, path string) *artifact.Entry {
	for _, entry := range result.Entries {
		if entry.Field == field && entry.Path == path {
			return entry
		}
	}
	return nil
}

func (d *diffControllerTestSuite) entriesByField(result *artifact.DiffResult, field artifact.DiffField) []*artifact.Entry {
	var entries []*artifact.Entry
	for _, entry := range result.Entries {
		if entry.Field == field {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (d *diffControllerTestSuite) entryPaths(entries []*artifact.Entry) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	return paths
}

func TestDiffControllerTestSuite(t *testing.T) {
	suite.Run(t, &diffControllerTestSuite{})
}
