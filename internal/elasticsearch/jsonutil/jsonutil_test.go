// Copyright © 2026 Meroxa, Inc. and Miquido
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jsonutil

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteStringMatchesEncodingJSON proves WriteString escapes byte-for-byte
// like encoding/json, so the direct bulk request writes are indistinguishable
// from the struct field marshaling they replaced.
func TestWriteStringMatchesEncodingJSON(t *testing.T) {
	cases := []string{
		"",
		"plain",
		`quote " and backslash \`,
		"line\nbreak\tand\rcarriage",
		"\b\f",
		"html <>& chars",
		"unicode: héllo 世界",
		"line separator \u2028 and paragraph \u2029",
		"control \x01\x1f bytes",
		"emoji 🚀",
		"nested {json} [array] : colon , comma",
	}

	for _, c := range cases {
		want, err := json.Marshal(c)
		require.NoError(t, err)

		var got bytes.Buffer
		WriteString(&got, c)

		require.Equal(t, string(want), got.String(), "input %q", c)
	}
}

// TestWriteStringInvalidUTF8 covers a lone invalid byte, which encoding/json
// replaces with U+FFFD.
func TestWriteStringInvalidUTF8(t *testing.T) {
	in := "bad \xff byte"
	want, err := json.Marshal(in)
	require.NoError(t, err)

	var got bytes.Buffer
	WriteString(&got, in)

	require.Equal(t, string(want), got.String())
}

// TestWriteStringDoesNotAllocate is the reason this package exists: the bulk
// builder writes many IDs per batch and must not allocate one per field.
func TestWriteStringDoesNotAllocate(t *testing.T) {
	var w bytes.Buffer
	w.Grow(256)

	allocs := testing.AllocsPerRun(1000, func() {
		w.Reset()
		WriteString(&w, "some-document-id-123")
	})
	require.Equal(t, 0.0, allocs)
}
