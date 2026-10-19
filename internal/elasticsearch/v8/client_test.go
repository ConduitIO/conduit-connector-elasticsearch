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

package v8

import (
	"bytes"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
)

const indexName = "someIndexName"

func TestNewClient(t *testing.T) {
	t.Run("Fails when provided config object is invalid", func(t *testing.T) {
		client, err := NewClient("invalid config object")

		require.Nil(t, client)
		require.EqualError(t, err, "provided config object is invalid")
	})
}

func TestClient_GetClient(t *testing.T) {
	esClient := &elasticsearch.Client{}

	client := Client{
		es: esClient,
	}

	require.Same(t, esClient, client.GetClient())
}

func TestClient_WriteCreateOperation(t *testing.T) {
	client := Client{cfg: &configMock{}}

	t.Run("Fails when payload could not be prepared", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteCreateOperation(&w, sdk.SourceUtil{}.NewRecordCreate(
			nil,
			nil,
			nil,
			opencdc.StructuredData{
				"foo": complex64(1 + 2i),
			},
		), indexName)

		require.EqualError(t, err, "json: unsupported type: complex64")
	})

	t.Run("Successfully writes create operation", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteCreateOperation(&w, sdk.SourceUtil{}.NewRecordCreate(
			nil,
			nil,
			nil,
			opencdc.StructuredData{"foo": "bar"},
		), indexName)

		require.NoError(t, err)
		require.Equal(t,
			`{"create":{"_index":"someIndexName"}}`+"\n"+`{"foo":"bar"}`+"\n",
			w.String())
	})
}

func TestClient_WriteUpsertOperation(t *testing.T) {
	client := Client{cfg: &configMock{}}

	t.Run("Fails when payload could not be prepared", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteUpsertOperation(
			&w,
			"key",
			sdk.SourceUtil{}.NewRecordUpdate(
				nil,
				nil,
				nil,
				opencdc.StructuredData{
					"foo": complex64(12 + 2i),
				},
				opencdc.StructuredData{
					"foo": complex64(1 + 2i),
				},
			),
			indexName,
		)

		require.EqualError(t, err, "json: unsupported type: complex64")
	})

	t.Run("Successfully writes upsert operation", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteUpsertOperation(
			&w,
			"key",
			sdk.SourceUtil{}.NewRecordUpdate(
				nil,
				nil,
				nil,
				opencdc.StructuredData{"foo": "bar"},
				opencdc.StructuredData{"foo": "baz"},
			),
			indexName,
		)

		require.NoError(t, err)
		require.Equal(t,
			`{"update":{"_id":"key","_index":"someIndexName","retry_on_conflict":3}}`+"\n"+
				`{"doc":{"foo":"baz"},"doc_as_upsert":true}`+"\n",
			w.String())
	})
}

func TestClient_WritePayloadEmptyWritesNull(t *testing.T) {
	client := Client{cfg: &configMock{}}

	// SDK #411 leaves an empty (zero-length) or nil payload part as absent
	// instead of decoding it into an invented all-null record. preparePayload
	// must write JSON null, not an empty document, so the record still reaches
	// Elasticsearch (invariant 3) without inventing data (invariant 6).
	t.Run("create with empty RawData", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteCreateOperation(
			&w,
			sdk.SourceUtil{}.NewRecordCreate(nil, nil, nil, opencdc.RawData{}),
			indexName,
		)
		require.NoError(t, err)
		require.Equal(t,
			`{"create":{"_index":"someIndexName"}}`+"\n"+"null\n",
			w.String())
	})

	t.Run("create with nil payload", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteCreateOperation(
			&w,
			sdk.SourceUtil{}.NewRecordCreate(nil, nil, nil, nil),
			indexName,
		)
		require.NoError(t, err)
		require.Equal(t,
			`{"create":{"_index":"someIndexName"}}`+"\n"+"null\n",
			w.String())
	})

	t.Run("upsert with empty RawData", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteUpsertOperation(
			&w,
			"key",
			sdk.SourceUtil{}.NewRecordUpdate(nil, nil, nil, nil, opencdc.RawData{}),
			indexName,
		)
		require.NoError(t, err)
		require.Equal(t,
			`{"update":{"_id":"key","_index":"someIndexName","retry_on_conflict":3}}`+"\n"+
				`{"doc":null,"doc_as_upsert":true}`+"\n",
			w.String())
	})
}

func TestClient_WriteUpsertCompactsRawData(t *testing.T) {
	client := Client{cfg: &configMock{}}

	// RawData is re-marshaled through json.RawMessage so that insignificant
	// whitespace is stripped (keeping the bulk request newline-delimited) and
	// HTML characters are escaped exactly as the previous struct-based
	// encoding did.
	var w bytes.Buffer
	rec := sdk.SourceUtil{}.NewRecordUpdate(nil, nil, nil, nil, opencdc.RawData(`{"x":"<b>&",  "ws" :  1}`))
	err := client.WriteUpsertOperation(&w, "key", rec, indexName)

	require.NoError(t, err)
	require.Equal(t,
		`{"update":{"_id":"key","_index":"someIndexName","retry_on_conflict":3}}`+"\n"+
			`{"doc":{"x":"\u003cb\u003e\u0026","ws":1},"doc_as_upsert":true}`+"\n",
		w.String())
}

func TestClient_WriteDeleteOperation(t *testing.T) {
	client := Client{cfg: &configMock{}}

	t.Run("Successfully writes delete operation", func(t *testing.T) {
		var w bytes.Buffer
		err := client.WriteDeleteOperation(&w, "key", indexName)

		require.NoError(t, err)
		require.Equal(t,
			`{"delete":{"_id":"key","_index":"someIndexName"}}`+"\n",
			w.String())
	})
}
