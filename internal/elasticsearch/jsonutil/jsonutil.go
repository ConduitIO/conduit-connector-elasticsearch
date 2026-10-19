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

// Package jsonutil provides allocation-free helpers for writing JSON. The bulk
// request builder writes document IDs and index names straight into the shared
// request buffer; escaping them without the round-trip through json.Marshal
// avoids one heap allocation per field per record.
package jsonutil

import (
	"bytes"
	"unicode/utf8"
)

const hexDigits = "0123456789abcdef"

// WriteString writes s to w as a JSON string literal (quoted and escaped). The
// escaping is byte-for-byte what encoding/json produces for a string field
// with its default HTML escaping enabled, so switching the bulk request from
// struct marshaling to direct writes does not change what reaches
// Elasticsearch.
func WriteString(w *bytes.Buffer, s string) {
	w.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			// Safe ASCII bytes are copied in one run below.
			if b >= 0x20 && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			w.WriteString(s[start:i])
			switch b {
			case '"':
				w.WriteString(`\"`)
			case '\\':
				w.WriteString(`\\`)
			case '\b':
				w.WriteString(`\b`)
			case '\f':
				w.WriteString(`\f`)
			case '\n':
				w.WriteString(`\n`)
			case '\r':
				w.WriteString(`\r`)
			case '\t':
				w.WriteString(`\t`)
			case '<':
				w.WriteString(`\u003c`)
			case '>':
				w.WriteString(`\u003e`)
			case '&':
				w.WriteString(`\u0026`)
			default:
				// Remaining control bytes below 0x20.
				w.WriteString(`\u00`)
				w.WriteByte(hexDigits[b>>4])
				w.WriteByte(hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			// Invalid UTF-8 byte: encoding/json writes U+FFFD.
			w.WriteString(s[start:i])
			w.WriteString(`\ufffd`)
			i++
			start = i
		case r == '\u2028' || r == '\u2029':
			// LINE/PARAGRAPH SEPARATOR are valid JSON but escaped for safety.
			w.WriteString(s[start:i])
			if r == '\u2028' {
				w.WriteString(`\u2028`)
			} else {
				w.WriteString(`\u2029`)
			}
			i += size
			start = i
		default:
			i += size
		}
	}
	w.WriteString(s[start:])
	w.WriteByte('"')
}
