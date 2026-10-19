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

package lwp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxDeletionsPerRepositoryRoundTrip(t *testing.T) {
	// omitted threshold stays nil
	meta := &Metadata{Algorithm: "or"}
	jsonData, err := meta.ToJSON()
	require.NoError(t, err)

	decoded := &Metadata{}
	require.NoError(t, decoded.FromJSON(jsonData))
	require.Nil(t, decoded.MaxDeletionsPerRepository)

	// explicit zero is preserved after the job parameter round trip
	zero := 0
	meta.MaxDeletionsPerRepository = &zero
	jsonData, err = meta.ToJSON()
	require.NoError(t, err)

	decoded = &Metadata{}
	require.NoError(t, decoded.FromJSON(jsonData))
	require.NotNil(t, decoded.MaxDeletionsPerRepository)
	require.Equal(t, 0, *decoded.MaxDeletionsPerRepository)
}
