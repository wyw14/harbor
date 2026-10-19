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
	"testing"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/lib/q"
)

func diffSnapshots() (*Snapshot, *Snapshot) {
	left := &Snapshot{
		Source:            Source{ProjectName: "library", RepositoryName: "library/app", Digest: "sha256:parent"},
		ManifestMediaType: v1.MediaTypeImageIndex,
		Tags:              []string{"v1", "common"},
		Platforms:         []string{"linux/amd64", "linux/arm64"},
		Signatures: []SignatureRef{
			{Digest: "sha256:sig-common", Type: "signature.cosign"},
			{Digest: "sha256:sig-left", Type: "signature.cosign"},
		},
		ScanSummary: map[string]any{
			"scan_status": "Success",
			"severity":    "High",
			"only_left":   "x",
		},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	right := &Snapshot{
		Source:            Source{ProjectName: "docker", RepositoryName: "docker/app", Digest: "sha256:parent"},
		ManifestMediaType: v1.MediaTypeImageIndex,
		Tags:              []string{"v2", "common"},
		Platforms:         []string{"linux/amd64"},
		Signatures: []SignatureRef{
			{Digest: "sha256:sig-common", Type: "signature.cosign"},
			{Digest: "sha256:sig-right", Type: "signature.notation"},
		},
		ScanSummary: map[string]any{
			"scan_status": "Success",
			"severity":    "Low",
			"only_right":  42,
		},
		CreatedAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}
	return left, right
}

func findEntry(entries []*Entry, field DiffField, path string) *Entry {
	for _, entry := range entries {
		if entry.Field == field && entry.Path == path {
			return entry
		}
	}
	return nil
}

func TestCompareTagsAndProvenance(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, nil)

	v1 := findEntry(result.Entries, DiffFieldTags, "v1")
	require.NotNil(t, v1, "tag only on the left must be reported")
	assert.Equal(t, DiffSideLeft, v1.Side)
	assert.Equal(t, "library/app", v1.Source.RepositoryName)
	assert.Equal(t, "v1", v1.Left)
	assert.Nil(t, v1.Right)

	v2 := findEntry(result.Entries, DiffFieldTags, "v2")
	require.NotNil(t, v2)
	assert.Equal(t, DiffSideRight, v2.Side)
	assert.Equal(t, "docker/app", v2.Source.RepositoryName)

	assert.Nil(t, findEntry(result.Entries, DiffFieldTags, "common"), "the shared tag must not be reported")
}

func TestComparePlatforms(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, nil)

	arm64 := findEntry(result.Entries, DiffFieldPlatforms, "linux/arm64")
	require.NotNil(t, arm64, "the platform missing on the right must be reported")
	assert.Equal(t, DiffSideLeft, arm64.Side)
	assert.Equal(t, "linux/arm64", arm64.Left)

	assert.Nil(t, findEntry(result.Entries, DiffFieldPlatforms, "linux/amd64"))
}

func TestCompareSignatures(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, nil)

	sigLeft := findEntry(result.Entries, DiffFieldSignatures, "sha256:sig-left")
	require.NotNil(t, sigLeft)
	assert.Equal(t, DiffSideLeft, sigLeft.Side)

	sigRight := findEntry(result.Entries, DiffFieldSignatures, "sha256:sig-right")
	require.NotNil(t, sigRight)
	assert.Equal(t, DiffSideRight, sigRight.Side)

	assert.Nil(t, findEntry(result.Entries, DiffFieldSignatures, "sha256:sig-common"))
}

// The same signature digest recorded with a different signature type is a value
// difference attributed to both sides rather than an addition/removal.
func TestCompareSignatureTypeMismatch(t *testing.T) {
	left := &Snapshot{Source: Source{RepositoryName: "l"}, Signatures: []SignatureRef{
		{Digest: "sha256:sig", Type: "signature.cosign"},
	}}
	right := &Snapshot{Source: Source{RepositoryName: "r"}, Signatures: []SignatureRef{
		{Digest: "sha256:sig", Type: "signature.notation"},
	}}
	result := Compare(left, right, nil)
	entry := findEntry(result.Entries, DiffFieldSignatures, "sha256:sig")
	require.NotNil(t, entry)
	assert.Equal(t, DiffSideBoth, entry.Side)
	assert.Equal(t, "signature.cosign", entry.Left.(SignatureRef).Type)
	assert.Equal(t, "signature.notation", entry.Right.(SignatureRef).Type)
}

func TestCompareScanSummaryPerKey(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, nil)

	severity := findEntry(result.Entries, DiffFieldScanSummary, "scan_summary.severity")
	require.NotNil(t, severity, "the differing summary key must be reported per key")
	assert.Equal(t, "High", severity.Left)
	assert.Equal(t, "Low", severity.Right)
	assert.Nil(t, severity.Source, "a difference on both sides is attributed to neither source")

	onlyLeft := findEntry(result.Entries, DiffFieldScanSummary, "scan_summary.only_left")
	require.NotNil(t, onlyLeft)
	assert.Equal(t, "x", onlyLeft.Left)
	assert.Nil(t, onlyLeft.Right)
	assert.Equal(t, "library/app", onlyLeft.Source.RepositoryName)

	onlyRight := findEntry(result.Entries, DiffFieldScanSummary, "scan_summary.only_right")
	require.NotNil(t, onlyRight)
	assert.Equal(t, 42, onlyRight.Right)

	assert.Nil(t, findEntry(result.Entries, DiffFieldScanSummary, "scan_summary.scan_status"),
		"equal keys must not be reported")
}

func TestCompareCreatedAt(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, nil)
	entry := findEntry(result.Entries, DiffFieldCreatedAt, "created_at")
	require.NotNil(t, entry)
	assert.Equal(t, DiffSideBoth, entry.Side)
	assert.Equal(t, "2026-01-01T00:00:00Z", entry.Left)
	assert.Equal(t, "2026-02-01T00:00:00Z", entry.Right)
}

func TestCompareIdenticalSnapshots(t *testing.T) {
	left, _ := diffSnapshots()
	right := *left
	right.Source = Source{ProjectName: "library", RepositoryName: "library/app-mirror", Digest: "sha256:parent"}
	result := Compare(left, &right, nil)
	assert.Empty(t, result.Entries)
	assert.Equal(t, int64(0), result.Total)
	assert.Empty(t, result.MissingChildren)
}

// Missing sub-manifests are reported in a dedicated section and must not leak
// into the regular field entries or the paginated total.
func TestCompareMissingChildrenMarkedSeparately(t *testing.T) {
	left, right := diffSnapshots()
	left.Children = []ChildManifest{
		{
			Digest:   "sha256:missing-in-db",
			Platform: &v1.Platform{OS: "linux", Architecture: "amd64"},
			Missing:  true,
			Reason:   MissingInDatabase,
		},
	}
	right.Children = []ChildManifest{
		{
			Digest:   "sha256:missing-in-registry",
			Platform: &v1.Platform{OS: "linux", Architecture: "arm64"},
			Missing:  true,
			Reason:   MissingInRegistry,
		},
		{
			Digest:   "sha256:present",
			Platform: &v1.Platform{OS: "linux", Architecture: "ppc64le"},
			Missing:  false,
		},
	}
	result := Compare(left, right, nil)

	require.Len(t, result.MissingChildren, 2)
	assert.Equal(t, DiffSideLeft, result.MissingChildren[0].Side)
	assert.Equal(t, "sha256:missing-in-db", result.MissingChildren[0].Child.Digest)
	assert.Equal(t, MissingInDatabase, result.MissingChildren[0].Child.Reason)
	assert.Equal(t, "sha256:parent", result.MissingChildren[0].ParentDigest)
	assert.Equal(t, DiffSideRight, result.MissingChildren[1].Side)
	assert.Equal(t, MissingInRegistry, result.MissingChildren[1].Child.Reason)

	for _, entry := range result.Entries {
		assert.NotContains(t, entry.Path, "missing-in-db")
		assert.NotContains(t, entry.Path, "missing-in-registry")
	}
}

// Default ordering is field order, then path ascending; the total counts all
// entries regardless of pagination.
func TestCompareDefaultSortingAndPagination(t *testing.T) {
	left := &Snapshot{Source: Source{RepositoryName: "l"},
		Tags:      []string{"c", "a", "b"},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	right := &Snapshot{Source: Source{RepositoryName: "r"},
		CreatedAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)}
	query := &q.Query{PageNumber: 1, PageSize: 2}
	result := Compare(left, right, query)

	assert.Equal(t, int64(4), result.Total, "3 tag entries + 1 created_at entry")
	require.Len(t, result.Entries, 2, "first page holds two entries")
	assert.Equal(t, DiffFieldTags, result.Entries[0].Field)
	assert.Equal(t, "a", result.Entries[0].Path)
	assert.Equal(t, "b", result.Entries[1].Path)

	query = &q.Query{PageNumber: 2, PageSize: 2}
	result = Compare(left, right, query)
	require.Len(t, result.Entries, 2)
	assert.Equal(t, "c", result.Entries[0].Path)
	assert.Equal(t, DiffFieldCreatedAt, result.Entries[1].Field, "created_at sorts after tags")

	// page beyond the end is an empty page, not an error
	query = &q.Query{PageNumber: 9, PageSize: 2}
	result = Compare(left, right, query)
	assert.Empty(t, result.Entries)
	assert.Equal(t, int64(4), result.Total)

	// zero page size returns everything
	result = Compare(left, right, &q.Query{})
	assert.Len(t, result.Entries, 4)
}

func TestCompareSortByPathDescending(t *testing.T) {
	left := &Snapshot{Source: Source{RepositoryName: "l"}, Tags: []string{"a", "b", "c"}}
	right := &Snapshot{Source: Source{RepositoryName: "r"}}
	result := Compare(left, right, &q.Query{Sorts: []*q.Sort{q.NewSort("path", true)}})
	require.Len(t, result.Entries, 3)
	assert.Equal(t, "c", result.Entries[0].Path)
	assert.Equal(t, "b", result.Entries[1].Path)
	assert.Equal(t, "a", result.Entries[2].Path)
}

func TestCompareSortByFieldDescending(t *testing.T) {
	left, right := diffSnapshots()
	result := Compare(left, right, &q.Query{Sorts: []*q.Sort{q.NewSort("field", true)}})
	require.NotEmpty(t, result.Entries)
	// created_at is the last field in ascending order, so descending puts it first
	assert.Equal(t, DiffFieldCreatedAt, result.Entries[0].Field)
}

func TestPlatformKey(t *testing.T) {
	assert.Equal(t, "", PlatformKey(nil))
	assert.Equal(t, "linux/amd64", PlatformKey(&v1.Platform{OS: "linux", Architecture: "amd64"}))
	assert.Equal(t, "linux/arm64/v7", PlatformKey(&v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v7"}))
}
