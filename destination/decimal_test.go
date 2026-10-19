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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/decimaltext"
	v8 "github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch/v8"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/stretchr/testify/require"
)

// bulkRecorder is a client that prepares operations like the v8 client and
// records the bulk bodies it is sent, answering every item with 200.
func bulkRecorder(t *testing.T, bodies *[]string) *clientMock {
	t.Helper()
	es := &v8.Client{}
	return &clientMock{
		WriteCreateOperationFunc: es.WriteCreateOperation,
		WriteUpsertOperationFunc: es.WriteUpsertOperation,
		WriteDeleteOperationFunc: es.WriteDeleteOperation,
		BulkFunc: func(_ context.Context, reader io.Reader) (io.ReadCloser, error) {
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			*bodies = append(*bodies, string(body))
			n := bytes.Count(body, []byte("\n")) / 2
			items := make([]bulkResponseItems, 0, n)
			for range n {
				items = append(items, bulkResponseItems{Update: &bulkResponseItem{Status: http.StatusOK}})
			}
			data, err := json.Marshal(bulkResponse{Items: items})
			require.NoError(t, err)
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}
}

func decimalDestination(c *clientMock) *Destination {
	return &Destination{
		getIndexName: func(opencdc.Record) (string, error) { return indexName, nil },
		client:       c,
	}
}

func TestWrite_DecimalWithoutExactTextIsNotWritten(t *testing.T) {
	var bodies []string
	c := bulkRecorder(t, &bodies)
	d := decimalDestination(c)

	ok := opencdc.Record{
		Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k1"),
		Payload: opencdc.Change{After: opencdc.StructuredData{"f": big.NewRat(3, 2)}},
	}
	bad := opencdc.Record{
		Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k2"),
		Payload: opencdc.Change{After: opencdc.StructuredData{"f": big.NewRat(1, 3)}},
	}

	// Invariant 6 and 1: nothing is sent and nothing is reported written,
	// rather than a rounded or ratio value reaching Elasticsearch.
	n, err := d.Write(context.Background(), []opencdc.Record{ok, bad})
	require.ErrorIs(t, err, decimaltext.ErrNotExact)
	require.ErrorContains(t, err, "record 1")
	require.Equal(t, 0, n)
	require.Empty(t, c.BulkCalls())
}

func TestWrite_DecimalsDoNotMutateRecords(t *testing.T) {
	var bodies []string
	d := decimalDestination(bulkRecorder(t, &bodies))

	inner := map[string]any{"d": big.NewRat(-1, 8)}
	payload := opencdc.StructuredData{"f": big.NewRat(3, 2), "r": inner, "a": []any{big.NewRat(1, 4)}}
	records := []opencdc.Record{
		{Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k1"), Payload: opencdc.Change{After: payload}},
		{Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k2"), Payload: opencdc.Change{After: opencdc.StructuredData{"x": 1}}},
	}

	n, err := d.Write(context.Background(), records)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	// No schema in the metadata: the fewest exact digits.
	require.Equal(t,
		`{"update":{"_id":"k1","_index":"someIndexName","retry_on_conflict":3}}`+"\n"+
			`{"doc":{"a":["0.25"],"f":"1.5","r":{"d":"-0.125"}},"doc_as_upsert":true}`+"\n"+
			`{"update":{"_id":"k2","_index":"someIndexName","retry_on_conflict":3}}`+"\n"+
			`{"doc":{"x":1},"doc_as_upsert":true}`+"\n",
		bodies[0])

	// The SDK may hand the same records to other middleware or retry them.
	require.IsType(t, &big.Rat{}, payload["f"])
	require.IsType(t, &big.Rat{}, inner["d"])
	require.IsType(t, &big.Rat{}, payload["a"].([]any)[0])
	require.Same(t, payload["r"].(map[string]any)["d"], inner["d"])
}

func TestWrite_DecimalSchemaLookupFailureIsNotWritten(t *testing.T) {
	var bodies []string
	c := bulkRecorder(t, &bodies)
	d := decimalDestination(c)

	r := opencdc.Record{
		Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k1"),
		Metadata: opencdc.Metadata{},
		Payload:  opencdc.Change{After: opencdc.StructuredData{"f": big.NewRat(3, 2)}},
	}
	r.Metadata.SetPayloadSchemaSubject("decimal-test.does-not-exist")
	r.Metadata.SetPayloadSchemaVersion(1)

	// Without the schema the scale is unknown; writing "1.5" for a field that
	// is "1.50" everywhere else would make the output depend on the schema
	// service, so the batch fails with nothing written. The SDK does not retry
	// Write: the nack stops the pipeline (default DLQ) or sends the batch to
	// the DLQ (nack threshold above 0).
	n, err := d.Write(context.Background(), []opencdc.Record{r})
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, c.BulkCalls())
}

func TestWrite_DecimalUsesSchemaScale(t *testing.T) {
	ctx := context.Background()
	var bodies []string
	d := decimalDestination(bulkRecorder(t, &bodies))

	sch, err := schema.Create(ctx, schema.TypeAvro, "decimal-test.scale",
		[]byte(`{"type":"record","name":"p","fields":[{"name":"f","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}}]}`))
	require.NoError(t, err)

	r := opencdc.Record{
		Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k1"),
		Metadata: opencdc.Metadata{},
		Payload:  opencdc.Change{After: opencdc.StructuredData{"f": big.NewRat(3, 2)}},
	}
	r.Metadata.SetPayloadSchemaSubject(sch.Subject)
	r.Metadata.SetPayloadSchemaVersion(sch.Version)

	for range 2 { // the second write takes the parsed schema from the memo
		n, err := d.Write(ctx, []opencdc.Record{r})
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	require.Len(t, bodies, 2)
	require.Contains(t, bodies[0], `{"doc":{"f":"1.5000"},"doc_as_upsert":true}`)
	require.Equal(t, bodies[0], bodies[1])
}

func TestWrite_DecimalSchemaThatDoesNotParseAloneIsPermanent(t *testing.T) {
	ctx := context.Background()
	var bodies []string
	c := bulkRecorder(t, &bodies)
	d := decimalDestination(c)

	// Another schema in the process defines the named fixed. commons parses
	// with the codec's package-level cache, so a schema that only refers to it
	// is accepted there; parsed on its own it does not resolve.
	_, err := schema.Create(ctx, schema.TypeAvro, "decimal-test.defines-fx",
		[]byte(`{"type":"record","name":"d","fields":[{"name":"f","type":{"type":"fixed","name":"leakfx","size":8,"logicalType":"decimal","precision":10,"scale":2}}]}`))
	require.NoError(t, err)
	sch, err := schema.Create(ctx, schema.TypeAvro, "decimal-test.refers-fx",
		[]byte(`{"type":"record","name":"p","fields":[{"name":"f","type":"leakfx"}]}`))
	require.NoError(t, err)

	r := opencdc.Record{
		Operation: opencdc.OperationUpdate, Key: opencdc.RawData("k1"),
		Metadata: opencdc.Metadata{},
		Payload:  opencdc.Change{After: opencdc.StructuredData{"f": big.NewRat(3, 2)}},
	}
	r.Metadata.SetPayloadSchemaSubject(sch.Subject)
	r.Metadata.SetPayloadSchemaVersion(sch.Version)

	for range 2 { // permanent: the same on every attempt
		n, err := d.Write(ctx, []opencdc.Record{r})
		require.ErrorContains(t, err, "permanent")
		require.Equal(t, 0, n)
	}
	require.Empty(t, c.BulkCalls())
}
