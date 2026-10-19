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

package destination

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// benchRecords returns n keyed update records with a realistic structured
// payload. This is the common CDC path: a key (document ID) plus a payload that
// is serialized to JSON per write.
func benchRecords(n int) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		recs[i] = sdk.SourceUtil{}.NewRecordUpdate(
			opencdc.Position(fmt.Sprintf("p%d", i)),
			nil,
			opencdc.RawData(fmt.Sprintf("key-%d", i)),
			nil,
			opencdc.StructuredData{
				"id":    i,
				"name":  fmt.Sprintf("item-%d", i),
				"price": 12.34,
				"tags":  []string{"a", "b", "c"},
			},
		)
	}
	return recs
}

// newBenchDestination returns a Destination wired to a real v8 client whose
// transport never actually needs to be reached (the request-building path is
// pure), plus an IndexFn resolving to a static index.
func newBenchDestination() *Destination {
	d := &Destination{
		config: Config{
			Version: elasticsearch.Version8,
			Host:    "http://127.0.0.1:9200",
		},
		getIndexName: func(opencdc.Record) (string, error) { return indexName, nil },
	}
	client, err := elasticsearch.NewClient(elasticsearch.Version8, d.config)
	if err != nil {
		panic(err)
	}
	d.client = client
	return d
}

// BenchmarkPrepareBulkRequestPayload measures the per-record allocation of the
// pure request-building hot path (no network): index resolution, key
// conversion, operation preparation and JSON encoding.
func BenchmarkPrepareBulkRequestPayload(b *testing.B) {
	d := newBenchDestination()
	recs := benchRecords(1000)

	b.ReportAllocs()
	b.SetBytes(int64(len(recs)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.prepareBulkRequestPayload(recs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWrite measures the full Write path including the HTTP round-trip to
// a local fake server and the bulk-response parse.
func BenchmarkWrite(b *testing.B) {
	d := newBenchDestination()
	recs := benchRecords(1000)

	// Precompute a valid bulk response with one ok update item per record, so
	// the handler allocates nothing per request and the benchmark measures the
	// connector's own path.
	body := []byte(`{"took":0,"errors":false,"items":[` + okItems(len(recs)) + `]}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// go-elasticsearch v8 rejects servers that do not declare themselves.
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	d.config.Host = srv.URL
	client, err := elasticsearch.NewClient(elasticsearch.Version8, d.config)
	if err != nil {
		b.Fatal(err)
	}
	d.client = client

	b.ReportAllocs()
	b.SetBytes(int64(len(recs)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.Write(context.Background(), recs); err != nil {
			b.Fatal(err)
		}
	}
}

// okItems returns n comma-joined ok update items, e.g.
// `{"update":{"_id":"key-0","status":200}},...`.
func okItems(n int) string {
	// 1000 items is small; a simple append is clear and runs once per benchmark.
	s := make([]byte, 0, n*48)
	for i := 0; i < n; i++ {
		if i > 0 {
			s = append(s, ',')
		}
		s = append(s, `{"update":{"_id":"key-`...)
		s = strconv.AppendInt(s, int64(i), 10)
		s = append(s, `","status":200}}`...)
	}
	return string(s)
}
