// Copyright © 2022 Meroxa, Inc. and Miquido
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

package elasticsearch

import (
	"bytes"
	"context"
	"io"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch/api"
)

// Client describes Elasticsearch client interface.
type Client interface {
	// Ping executes Elasticsearch ping request.
	// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
	Ping(ctx context.Context) error

	// Bulk executes Elasticsearch Bulk API request.
	// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	Bulk(ctx context.Context, reader io.Reader) (io.ReadCloser, error)

	// WriteCreateOperation appends the create (index before 7.0) action and
	// source lines for a keyless record to w as two newline-delimited JSON
	// lines.
	WriteCreateOperation(w *bytes.Buffer, item opencdc.Record, index string) error

	// WriteUpsertOperation appends the update action and source lines for a
	// keyed record to w as two newline-delimited JSON lines.
	WriteUpsertOperation(w *bytes.Buffer, key string, item opencdc.Record, index string) error

	// WriteDeleteOperation appends the delete action line for a keyed record
	// to w as one newline-delimited JSON line.
	WriteDeleteOperation(w *bytes.Buffer, key string, index string) error

	// Search calls the elasticsearch search api and retuns SearchResponse read from an index.
	Search(ctx context.Context, request *api.SearchRequest) (*api.SearchResponse, error)
}
