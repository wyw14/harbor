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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/goharbor/harbor/src/lib/q"
)

// DiffSide identifies one side of a metadata diff.
type DiffSide string

const (
	// DiffSideLeft is the first target of a diff.
	DiffSideLeft DiffSide = "left"
	// DiffSideRight is the second target of a diff.
	DiffSideRight DiffSide = "right"
	// DiffSideBoth marks values that exist on both sides but differ.
	DiffSideBoth DiffSide = "both"
)

// DiffField identifies a compared metadata field.
type DiffField string

const (
	// DiffFieldTags compares the tag names attached to the artifact in each repository.
	DiffFieldTags DiffField = "tags"
	// DiffFieldPlatforms compares the platforms declared by multi-architecture indexes.
	DiffFieldPlatforms DiffField = "platforms"
	// DiffFieldSignatures compares the signature accessories (cosign/notation) referencing the artifact.
	DiffFieldSignatures DiffField = "signatures"
	// DiffFieldScanSummary compares the latest vulnerability scan summaries.
	DiffFieldScanSummary DiffField = "scan_summary"
	// DiffFieldCreatedAt compares the Harbor-side creation (push) time.
	DiffFieldCreatedAt DiffField = "created_at"
)

// diffFieldOrder pins the default ordering of fields in the result.
var diffFieldOrder = map[DiffField]int{
	DiffFieldTags:        1,
	DiffFieldPlatforms:   2,
	DiffFieldSignatures:  3,
	DiffFieldScanSummary: 4,
	DiffFieldCreatedAt:   5,
}

// Reasons a child manifest declared by an index can be missing on one side.
const (
	// MissingInDatabase means the child manifest is absent from Harbor's artifact metadata.
	MissingInDatabase = "missing_in_database"
	// MissingInRegistry means the backend registry no longer serves the child manifest.
	MissingInRegistry = "missing_in_registry"
)

// Source records where a metadata value was observed, i.e. the provenance of
// one side of the diff. The same digest can live in several projects with
// different tags, signatures and scan summaries, so every diff entry carries
// the source coordinates of its value.
type Source struct {
	ProjectName    string `json:"project_name"`
	RepositoryName string `json:"repository_name"`
	Digest         string `json:"digest"`
}

// SignatureRef is a read-only reference to a signature accessory.
type SignatureRef struct {
	Digest string `json:"digest"`
	Type   string `json:"type"`
}

// ChildManifest describes one manifest entry declared by a parent index.
// Missing is set separately from the regular field diffs when the declared
// child cannot be found on that side.
type ChildManifest struct {
	Digest      string            `json:"digest"`
	MediaType   string            `json:"media_type,omitempty"`
	Platform    *v1.Platform      `json:"platform,omitempty"`
	URLs        []string          `json:"urls,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	// Missing reports that the declared child is absent from Harbor metadata
	// (MissingInDatabase) or from the backend registry (MissingInRegistry).
	Missing bool   `json:"missing"`
	Reason  string `json:"reason,omitempty"`
}

// Snapshot is the read-only metadata view of one artifact side of the diff.
// It is assembled without triggering scans or changing tags.
type Snapshot struct {
	Source            Source          `json:"source"`
	ManifestMediaType string          `json:"manifest_media_type"`
	Tags              []string        `json:"tags"`
	Platforms         []string        `json:"platforms"`
	Signatures        []SignatureRef  `json:"signatures"`
	ScanSummary       map[string]any  `json:"scan_summary,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	Children          []ChildManifest `json:"children,omitempty"`
}

// Entry is one metadata difference. For values that exist on one side only,
// Side is DiffSideLeft/DiffSideRight and Source carries the provenance; for
// values present on both sides that differ, Side is DiffSideBoth and the two
// values are in Left/Right.
type Entry struct {
	Field  DiffField `json:"field"`
	Path   string    `json:"path"`
	Side   DiffSide  `json:"side"`
	Source *Source   `json:"source,omitempty"`
	Left   any       `json:"left,omitempty"`
	Right  any       `json:"right,omitempty"`
}

// MissingChild is a sub-manifest declared by an index that cannot be found on
// the reported side. It is intentionally kept separate from Entry so callers
// can surface metadata/registry gaps distinctly from value differences.
type MissingChild struct {
	Side         DiffSide      `json:"side"`
	Source       Source        `json:"source"`
	ParentDigest string        `json:"parent_digest"`
	Child        ChildManifest `json:"child"`
}

// DiffResult holds the paged field entries plus the separately marked missing
// sub-manifests. Total counts all entries before pagination.
type DiffResult struct {
	Entries         []*Entry        `json:"entries"`
	MissingChildren []*MissingChild `json:"missing_children"`
	Total           int64           `json:"total"`
}

// PlatformKey renders an OCI platform as the canonical os/arch[/variant] key
// used for platform comparison.
func PlatformKey(platform *v1.Platform) string {
	if platform == nil {
		return ""
	}
	key := fmt.Sprintf("%s/%s", platform.OS, platform.Architecture)
	if platform.Variant != "" {
		key += "/" + platform.Variant
	}
	return key
}

// Compare computes the read-only metadata diff of two artifact snapshots.
// The query only controls sorting ("field", "path") and pagination
// (PageNumber/PageSize); it must not contain any filter keywords.
func Compare(left, right *Snapshot, query *q.Query) *DiffResult {
	result := &DiffResult{Entries: []*Entry{}, MissingChildren: []*MissingChild{}}

	diffTags(result, left, right)
	diffPlatforms(result, left, right)
	diffSignatures(result, left, right)
	diffScanSummary(result, left, right)
	diffCreatedAt(result, left, right)
	collectMissingChildren(result, left, right)

	sortEntries(result.Entries, query)
	result.Total = int64(len(result.Entries))
	result.Entries = paginateEntries(result.Entries, query)
	return result
}

func diffTags(result *DiffResult, left, right *Snapshot) {
	leftTags := toSet(left.Tags)
	rightTags := toSet(right.Tags)
	for tag := range leftTags {
		if _, exist := rightTags[tag]; !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldTags, tag, DiffSideLeft, &left.Source, tag))
		}
	}
	for tag := range rightTags {
		if _, exist := leftTags[tag]; !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldTags, tag, DiffSideRight, &right.Source, tag))
		}
	}
}

func diffPlatforms(result *DiffResult, left, right *Snapshot) {
	leftPlatforms := toSet(left.Platforms)
	rightPlatforms := toSet(right.Platforms)
	for platform := range leftPlatforms {
		if _, exist := rightPlatforms[platform]; !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldPlatforms, platform, DiffSideLeft, &left.Source, platform))
		}
	}
	for platform := range rightPlatforms {
		if _, exist := leftPlatforms[platform]; !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldPlatforms, platform, DiffSideRight, &right.Source, platform))
		}
	}
}

func diffSignatures(result *DiffResult, left, right *Snapshot) {
	leftSigs := map[string]SignatureRef{}
	for _, sig := range left.Signatures {
		leftSigs[sig.Digest] = sig
	}
	rightSigs := map[string]SignatureRef{}
	for _, sig := range right.Signatures {
		rightSigs[sig.Digest] = sig
	}
	for digest, sig := range leftSigs {
		rightSig, exist := rightSigs[digest]
		if !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldSignatures, digest, DiffSideLeft, &left.Source, sig))
			continue
		}
		// the same signature digest referenced with a different signature type is still a difference
		if sig.Type != rightSig.Type {
			result.Entries = append(result.Entries, &Entry{
				Field: DiffFieldSignatures,
				Path:  digest,
				Side:  DiffSideBoth,
				Left:  sig,
				Right: rightSig,
			})
		}
	}
	for digest, sig := range rightSigs {
		if _, exist := leftSigs[digest]; !exist {
			result.Entries = append(result.Entries, oneSidedEntry(DiffFieldSignatures, digest, DiffSideRight, &right.Source, sig))
		}
	}
}

func diffScanSummary(result *DiffResult, left, right *Snapshot) {
	keys := map[string]struct{}{}
	for key := range left.ScanSummary {
		keys[key] = struct{}{}
	}
	for key := range right.ScanSummary {
		keys[key] = struct{}{}
	}
	for key := range keys {
		leftValue, leftOK := left.ScanSummary[key]
		rightValue, rightOK := right.ScanSummary[key]
		if reflect.DeepEqual(leftValue, rightValue) {
			continue
		}
		entry := &Entry{
			Field: DiffFieldScanSummary,
			Path:  string(DiffFieldScanSummary) + "." + key,
			Side:  DiffSideBoth,
		}
		if leftOK {
			entry.Left = leftValue
		}
		if rightOK {
			entry.Right = rightValue
		}
		// a key that exists on only one side carries that side's provenance;
		// a value present on both sides but different is attributed to neither
		if leftOK && !rightOK {
			entry.Source = &left.Source
		}
		if rightOK && !leftOK {
			entry.Source = &right.Source
		}
		result.Entries = append(result.Entries, entry)
	}
}

func diffCreatedAt(result *DiffResult, left, right *Snapshot) {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return
	}
	result.Entries = append(result.Entries, &Entry{
		Field: DiffFieldCreatedAt,
		Path:  string(DiffFieldCreatedAt),
		Side:  DiffSideBoth,
		Left:  left.CreatedAt.UTC().Format(time.RFC3339Nano),
		Right: right.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func collectMissingChildren(result *DiffResult, left, right *Snapshot) {
	appendSide := func(snap *Snapshot, side DiffSide) {
		for _, child := range snap.Children {
			if !child.Missing {
				continue
			}
			result.MissingChildren = append(result.MissingChildren, &MissingChild{
				Side:         side,
				Source:       snap.Source,
				ParentDigest: snap.Source.Digest,
				Child:        child,
			})
		}
	}
	appendSide(left, DiffSideLeft)
	appendSide(right, DiffSideRight)
	// deterministic output: side, then digest
	sort.Slice(result.MissingChildren, func(i, j int) bool {
		if result.MissingChildren[i].Side != result.MissingChildren[j].Side {
			return result.MissingChildren[i].Side == DiffSideLeft
		}
		return result.MissingChildren[i].Child.Digest < result.MissingChildren[j].Child.Digest
	})
}

func oneSidedEntry(field DiffField, path string, side DiffSide, source *Source, value any) *Entry {
	entry := &Entry{
		Field:  field,
		Path:   path,
		Side:   side,
		Source: source,
	}
	if side == DiffSideLeft {
		entry.Left = value
	} else {
		entry.Right = value
	}
	return entry
}

// sortEntries sorts by the requested keys ("field"/"path"), falling back to
// the natural field order and path to keep the result deterministic.
func sortEntries(entries []*Entry, query *q.Query) {
	less := defaultLess
	if query != nil && len(query.Sorts) > 0 {
		var keys []string
		var descs []bool
		for _, s := range query.Sorts {
			if s == nil {
				continue
			}
			switch s.Key {
			case "field", "path":
				keys = append(keys, s.Key)
				descs = append(descs, s.DESC)
			}
		}
		if len(keys) > 0 {
			less = func(a, b *Entry) bool {
				for i, key := range keys {
					av, bv := entrySortValue(a, key), entrySortValue(b, key)
					if av == bv {
						continue
					}
					if descs[i] {
						return av > bv
					}
					return av < bv
				}
				// tie-break with the natural order, honoring the outermost direction
				return defaultLess(a, b)
			}
		}
	}
	sort.SliceStable(entries, func(i, j int) bool { return less(entries[i], entries[j]) })
}

func entrySortValue(entry *Entry, key string) string {
	if key == "field" {
		// order known fields naturally even when sorting by field
		return fmt.Sprintf("%02d|%s", diffFieldOrder[entry.Field], entry.Field)
	}
	return entry.Path
}

// defaultLess sorts by field order, then by path.
func defaultLess(a, b *Entry) bool {
	if a.Field != b.Field {
		return diffFieldOrder[a.Field] < diffFieldOrder[b.Field]
	}
	return strings.Compare(a.Path, b.Path) < 0
}

// paginateEntries applies 1-based pagination. A zero PageSize returns all
// entries and a zero/out-of-range PageNumber is treated as the first page.
func paginateEntries(entries []*Entry, query *q.Query) []*Entry {
	if query == nil || query.PageSize <= 0 {
		return entries
	}
	pageNumber := query.PageNumber
	if pageNumber <= 0 {
		pageNumber = 1
	}
	start := (pageNumber - 1) * query.PageSize
	if start >= int64(len(entries)) {
		return []*Entry{}
	}
	end := start + query.PageSize
	if end > int64(len(entries)) {
		end = int64(len(entries))
	}
	return entries[start:end]
}

func toSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item] = struct{}{}
	}
	return set
}
