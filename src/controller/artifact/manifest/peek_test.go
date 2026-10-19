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

package manifest

import (
	"testing"

	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tart "github.com/goharbor/harbor/src/testing/pkg/artifact"
)

const (
	multiArchIndexContent = `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.index.v1+json",
   "manifests": [
      {
         "mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 7143,
         "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
         "platform": {"architecture": "amd64", "os": "linux"},
         "annotations": {"org.opencontainers.image.version": "1.0"}
      },
      {
         "mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 7143,
         "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
         "platform": {"architecture": "arm64", "os": "linux", "variant": "v8"},
         "urls": ["https://example.com/arm64.manifest"]
      }
   ]
}`

	dockerManifestListContent = `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
   "manifests": [
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 7143,
         "digest": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
         "platform": {"architecture": "amd64", "os": "linux"}
      }
   ]
}`
)

// Peeking a multi-architecture OCI index returns every declared platform with
// its annotations and URLs, straight from the payload, without any database
// lookup (the injected artifact manager is never touched).
func TestPeekReferencesMultiArchIndex(t *testing.T) {
	descriptors, err := PeekReferences(v1.MediaTypeImageIndex, []byte(multiArchIndexContent))
	require.NoError(t, err)
	require.Len(t, descriptors, 2)

	assert.Equal(t, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", descriptors[0].Digest)
	assert.Equal(t, v1.MediaTypeImageManifest, descriptors[0].MediaType)
	require.NotNil(t, descriptors[0].Platform)
	assert.Equal(t, "amd64", descriptors[0].Platform.Architecture)
	assert.Equal(t, "linux", descriptors[0].Platform.OS)
	assert.Equal(t, "1.0", descriptors[0].Annotations["org.opencontainers.image.version"])

	assert.Equal(t, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", descriptors[1].Digest)
	require.NotNil(t, descriptors[1].Platform)
	assert.Equal(t, "arm64", descriptors[1].Platform.Architecture)
	assert.Equal(t, "v8", descriptors[1].Platform.Variant)
	assert.Equal(t, []string{"https://example.com/arm64.manifest"}, descriptors[1].URLs)
}

func TestPeekReferencesDockerManifestList(t *testing.T) {
	descriptors, err := PeekReferences(manifestlist.MediaTypeManifestList, []byte(dockerManifestListContent))
	require.NoError(t, err)
	require.Len(t, descriptors, 1)
	assert.Equal(t, "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", descriptors[0].Digest)
	require.NotNil(t, descriptors[0].Platform)
	assert.Equal(t, "linux/amd64", descriptors[0].Platform.OS+"/"+descriptors[0].Platform.Architecture)
}

// A plain image manifest references nothing, so peeking must return an empty
// slice instead of an error regardless of database state.
func TestPeekReferencesImageManifestReturnsEmpty(t *testing.T) {
	descriptors, err := PeekReferences(schema2.MediaTypeManifest, []byte(v2ManifestContent))
	require.NoError(t, err)
	assert.Empty(t, descriptors)

	// same contract through the adapter directly
	adapter, err := Get(schema2.MediaTypeManifest)
	require.NoError(t, err)
	_, isReferrer := adapter.(Referrer)
	assert.False(t, isReferrer)
}

func TestPeekReferencesUnknownMediaType(t *testing.T) {
	_, err := PeekReferences("application/vnd.example.unknown", []byte("{}"))
	assert.Error(t, err)
}

func TestPeekReferencesMalformedIndex(t *testing.T) {
	referrer, ok := NewIndex(&tart.Manager{}).(Referrer)
	require.True(t, ok)
	_, err := referrer.PeekReferences([]byte("{not json"))
	assert.Error(t, err)
}
