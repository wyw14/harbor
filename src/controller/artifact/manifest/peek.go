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
	"encoding/json"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// Descriptor is the read-only view of a child manifest declared by an index.
// It is parsed straight from the registry payload and never touches the
// database, so children Harbor does not know about yet are still reported.
type Descriptor struct {
	Digest      string
	MediaType   string
	Platform    *v1.Platform
	URLs        []string
	Annotations map[string]string
}

// Referrer is implemented by manifest formats that reference other manifests,
// such as the OCI image index and the docker manifest list.
type Referrer interface {
	// PeekReferences parses the child manifests declared by the raw payload.
	// Unlike Abstract it performs no database lookup and never creates
	// anything: a child that is missing from Harbor metadata is returned
	// instead of making the inspection fail.
	PeekReferences(content []byte) ([]Descriptor, error)
}

// PeekReferences parses the raw manifest payload identified by mediaType and
// returns the child manifest descriptors it declares. Manifest formats that
// never reference other manifests return an empty slice without error, so the
// caller does not need to know which formats are indexes.
func PeekReferences(mediaType string, content []byte) ([]Descriptor, error) {
	abstractor, err := Get(mediaType)
	if err != nil {
		return nil, err
	}
	referrer, ok := abstractor.(Referrer)
	if !ok {
		return nil, nil
	}
	return referrer.PeekReferences(content)
}

// PeekReferences implements Referrer for the OCI index / docker manifest list
// adapter. The JSON shapes of both formats are compatible, so a single
// v1.Index unmarshal covers both.
func (a *indexAbstractor) PeekReferences(content []byte) ([]Descriptor, error) {
	index := &v1.Index{}
	if err := json.Unmarshal(content, index); err != nil {
		return nil, err
	}
	descriptors := make([]Descriptor, 0, len(index.Manifests))
	for _, mani := range index.Manifests {
		descriptors = append(descriptors, Descriptor{
			Digest:      mani.Digest.String(),
			MediaType:   mani.MediaType,
			Platform:    mani.Platform,
			URLs:        mani.URLs,
			Annotations: mani.Annotations,
		})
	}
	return descriptors, nil
}
