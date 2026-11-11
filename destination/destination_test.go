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

package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

const indexName = "someIndexName"

func TestNewDestination(t *testing.T) {
	t.Run("New Destination can be created", func(t *testing.T) {
		// NewDestination wraps the connector in the SDK middleware, so the
		// concrete type is not *Destination.
		require.NotNil(t, NewDestination())
	})
}

func TestDestination_GetClient(t *testing.T) {
	clientMock := &clientMock{}

	destination := Destination{
		client: clientMock,
	}

	require.Same(t, clientMock, destination.GetClient())
}

func TestDestination_Write(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Does not perform any action when queue is empty", func(t *testing.T) {
		esClientMock := clientMock{}

		destination := Destination{
			config: Config{},
			getIndexName: func(_ opencdc.Record) (string, error) {
				return indexName, nil
			},
			client: &esClientMock,
		}

		n, err := destination.Write(context.Background(), nil)
		require.Equal(t, 0, n)
		require.NoError(t, err)
		require.Len(t, esClientMock.WriteCreateOperationCalls(), 0)
		require.Len(t, esClientMock.WriteUpsertOperationCalls(), 0)
		require.Len(t, esClientMock.WriteDeleteOperationCalls(), 0)
		require.Len(t, esClientMock.BulkCalls(), 0)
	})

	t.Run("Does not retry when there were no failures", func(t *testing.T) {
		var (
			operationMetadata = fakerInstance.Lorem().Sentence(6)
			operationPayload  = fakerInstance.Lorem().Sentence(6)
		)

		esClientMock := clientMock{
			WriteCreateOperationFunc: func(w *bytes.Buffer, _ opencdc.Record, _ string) error {
				if err := json.NewEncoder(w).Encode(operationMetadata); err != nil {
					return err
				}
				return json.NewEncoder(w).Encode(operationPayload)
			},

			BulkFunc: func(_ context.Context, reader io.Reader) (io.ReadCloser, error) {
				bulkRequest, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%q\n%q\n", operationMetadata, operationPayload), string(bulkRequest))

				data, err := json.Marshal(bulkResponse{
					Took:   0,
					Errors: false,
					Items: []bulkResponseItems{
						{
							Create: &bulkResponseItem{
								Status: http.StatusOK,
							},
						},
					},
				})
				require.NoError(t, err)

				return io.NopCloser(bytes.NewReader(data)), nil
			},
		}

		destination := Destination{
			config: Config{
				BulkSize: 1,
				Retries:  2,
			},
			getIndexName: func(_ opencdc.Record) (string, error) {
				return indexName, nil
			},
			client: &esClientMock,
		}

		n, err := destination.Write(
			context.Background(),
			[]opencdc.Record{
				sdk.SourceUtil{}.NewRecordCreate(
					nil,
					nil,
					nil,
					opencdc.StructuredData{
						"id": fakerInstance.Int32(),
					},
				),
			},
		)
		require.Equal(t, 1, n)
		require.NoError(t, err)
		require.Len(t, esClientMock.WriteCreateOperationCalls(), 1)
		require.Len(t, esClientMock.WriteUpsertOperationCalls(), 0)
		require.Len(t, esClientMock.WriteDeleteOperationCalls(), 0)
		require.Len(t, esClientMock.BulkCalls(), 1)
	})
}

// bulkItemOK and bulkItemFailed build bulk response items for update
// (upsert) operations, which is what records with a key and an update,
// create or snapshot operation are sent as.
func bulkItemOK(id string) bulkResponseItems {
	return bulkResponseItems{Update: &bulkResponseItem{ID: id, Status: http.StatusOK}}
}

func bulkItemFailed(id string) bulkResponseItems {
	return bulkResponseItems{Update: &bulkResponseItem{
		ID:     id,
		Status: http.StatusBadRequest,
		Error: &bulkResponseItemError{
			Type:     "mapper_parsing_exception",
			Reason:   "failed to parse field [x]",
			CausedBy: json.RawMessage(`{}`),
		},
	}}
}

// TestDestination_Write_FailedItemIsNeverReportedWritten is the regression
// test for #153. The SDK acks batch[:n] and nacks batch[n:] for the (n, err)
// returned by Write, so n must be the number of records written BEFORE the
// first failed item. Returning n+1 made the SDK ack the failed record
// (Invariant 1: never acknowledge a record before it is durably written).
func TestDestination_Write_FailedItemIsNeverReportedWritten(t *testing.T) {
	const size = 5

	tests := []struct {
		name     string
		items    []bulkResponseItems
		wantN    int
		wantFail string // key of the first failed record, empty if no failure
	}{
		{
			name:     "first item fails",
			items:    []bulkResponseItems{bulkItemFailed("k0"), bulkItemOK("k1"), bulkItemOK("k2"), bulkItemOK("k3"), bulkItemOK("k4")},
			wantN:    0,
			wantFail: "k0",
		},
		{
			name:     "middle item fails",
			items:    []bulkResponseItems{bulkItemOK("k0"), bulkItemOK("k1"), bulkItemFailed("k2"), bulkItemOK("k3"), bulkItemOK("k4")},
			wantN:    2,
			wantFail: "k2",
		},
		{
			name:     "last item fails",
			items:    []bulkResponseItems{bulkItemOK("k0"), bulkItemOK("k1"), bulkItemOK("k2"), bulkItemOK("k3"), bulkItemFailed("k4")},
			wantN:    4,
			wantFail: "k4",
		},
		{
			name:     "several items fail, the first one counts",
			items:    []bulkResponseItems{bulkItemOK("k0"), bulkItemFailed("k1"), bulkItemOK("k2"), bulkItemFailed("k3"), bulkItemOK("k4")},
			wantN:    1,
			wantFail: "k1",
		},
		{
			name:  "no item fails",
			items: []bulkResponseItems{bulkItemOK("k0"), bulkItemOK("k1"), bulkItemOK("k2"), bulkItemOK("k3"), bulkItemOK("k4")},
			wantN: size,
		},
		{
			// Item without an error object, e.g. a bare 429/503 status.
			name: "failed item without error details",
			items: []bulkResponseItems{
				bulkItemOK("k0"),
				{Update: &bulkResponseItem{ID: "k1", Status: http.StatusServiceUnavailable}},
				bulkItemOK("k2"), bulkItemOK("k3"), bulkItemOK("k4"),
			},
			wantN:    1,
			wantFail: "k1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDestination(t, tc.items)

			n, err := d.Write(context.Background(), testRecords(size))

			require.Equal(t, tc.wantN, n)
			if tc.wantFail == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "key_hash="+hashOf(tc.wantFail)+" ")
			// The contract with the SDK: records[n] is the failed one.
			require.Less(t, n, size, "a failed batch must never report every record written")
		})
	}
}

// TestDestination_Write_UnconfirmedRecordsAreNotReportedWritten covers the
// other ways a record can reach the end of Write without Elasticsearch
// confirming it: a response with fewer items than the request, and an item
// carrying none of index/create/update/delete.
func TestDestination_Write_UnconfirmedRecordsAreNotReportedWritten(t *testing.T) {
	const size = 4

	tests := []struct {
		name  string
		items []bulkResponseItems
		wantN int
	}{
		{
			name:  "response shorter than request",
			items: []bulkResponseItems{bulkItemOK("k0"), bulkItemOK("k1")},
			wantN: 2,
		},
		{
			name:  "empty response",
			items: nil,
			wantN: 0,
		},
		{
			name:  "item without operation details",
			items: []bulkResponseItems{bulkItemOK("k0"), {}, bulkItemOK("k2"), bulkItemOK("k3")},
			wantN: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDestination(t, tc.items)

			n, err := d.Write(context.Background(), testRecords(size))

			require.Error(t, err)
			require.Equal(t, tc.wantN, n)
		})
	}
}

// TestDestination_Write_RequestFailureReportsNothingWritten checks the
// non-item error paths: nothing is confirmed, so nothing may be acked.
func TestDestination_Write_RequestFailureReportsNothingWritten(t *testing.T) {
	t.Run("bulk request error", func(t *testing.T) {
		d := newTestDestination(t, nil)
		d.client.(*clientMock).BulkFunc = func(context.Context, io.Reader) (io.ReadCloser, error) {
			return nil, fmt.Errorf("connection refused")
		}

		n, err := d.Write(context.Background(), testRecords(3))
		require.Error(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("malformed bulk response", func(t *testing.T) {
		d := newTestDestination(t, nil)
		d.client.(*clientMock).BulkFunc = func(context.Context, io.Reader) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("not json"))), nil
		}

		n, err := d.Write(context.Background(), testRecords(3))
		require.Error(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("request cannot be prepared", func(t *testing.T) {
		d := newTestDestination(t, nil)
		d.client.(*clientMock).WriteUpsertOperationFunc = func(*bytes.Buffer, string, opencdc.Record, string) error {
			return fmt.Errorf("unsupported payload")
		}

		n, err := d.Write(context.Background(), testRecords(3))
		require.Error(t, err)
		require.Equal(t, 0, n)
		require.Empty(t, d.client.(*clientMock).BulkCalls(), "nothing may be sent when the request cannot be built")
	})
}

// newTestDestination returns a Destination whose bulk endpoint answers with
// the given items, in request order.
func newTestDestination(t *testing.T, items []bulkResponseItems) *Destination {
	t.Helper()

	return &Destination{
		getIndexName: func(opencdc.Record) (string, error) { return indexName, nil },
		client: &clientMock{
			WriteUpsertOperationFunc: func(w *bytes.Buffer, key string, _ opencdc.Record, _ string) error {
				if err := json.NewEncoder(w).Encode(map[string]string{"key": key}); err != nil {
					return err
				}
				return json.NewEncoder(w).Encode(map[string]string{"doc": key})
			},
			BulkFunc: func(context.Context, io.Reader) (io.ReadCloser, error) {
				data, err := json.Marshal(bulkResponse{Errors: true, Items: items})
				require.NoError(t, err)

				return io.NopCloser(bytes.NewReader(data)), nil
			},
		},
	}
}

// testRecords returns n keyed update records with keys k0..k(n-1).
func testRecords(n int) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		recs[i] = sdk.SourceUtil{}.NewRecordUpdate(
			opencdc.Position(fmt.Sprintf("p%d", i)),
			nil,
			opencdc.RawData(fmt.Sprintf("k%d", i)),
			nil,
			opencdc.StructuredData{"i": i},
		)
	}

	return recs
}

// TestDestination_Write_LongerResponseNeverExceedsBatch: the SDK slices
// batch[n:], so n must stay below len(records) when a write fails.
func TestDestination_Write_LongerResponseNeverExceedsBatch(t *testing.T) {
	items := []bulkResponseItems{bulkItemOK("k0"), bulkItemOK("k1"), bulkItemOK("k2"), bulkItemOK("k3")}

	t.Run("all ok", func(t *testing.T) {
		n, err := newTestDestination(t, items).Write(context.Background(), testRecords(2))
		require.NoError(t, err)
		require.Equal(t, 2, n)
	})

	t.Run("failure beyond the batch is ignored", func(t *testing.T) {
		items := append(items[:2:2], bulkItemFailed("k2"))
		n, err := newTestDestination(t, items).Write(context.Background(), testRecords(2))
		require.NoError(t, err)
		require.Equal(t, 2, n)
	})
}

type errReadCloser struct{ closed *bool }

func (errReadCloser) Read([]byte) (int, error) { return 0, fmt.Errorf("read failed") }
func (e errReadCloser) Close() error           { *e.closed = true; return nil }

func TestDestination_Write_ClosesBodyWhenReadFails(t *testing.T) {
	d := newTestDestination(t, nil)
	var closed bool
	d.client.(*clientMock).BulkFunc = func(context.Context, io.Reader) (io.ReadCloser, error) {
		return errReadCloser{closed: &closed}, nil
	}

	n, err := d.Write(context.Background(), testRecords(2))
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.True(t, closed)
}

// TestDestination_Write_404 is the regression test for #154. A 404 is only a
// confirmed outcome for delete (the document is already gone). For every
// other operation it means nothing was stored, e.g. the index does not exist
// and auto-creation is disabled, so the record must not be reported written.
func TestDestination_Write_404(t *testing.T) {
	notFound := func(id string) *bulkResponseItem {
		return &bulkResponseItem{
			ID:     id,
			Status: http.StatusNotFound,
			Error: &bulkResponseItemError{
				Type:     "index_not_found_exception",
				Reason:   "no such index [x] and [action.auto_create_index] contains [-x*]",
				CausedBy: json.RawMessage(`{}`),
			},
		}
	}

	tests := []struct {
		name    string
		item    bulkResponseItems
		wantN   int
		wantErr bool
	}{
		{name: "update 404 is a failure", item: bulkResponseItems{Update: notFound("k1")}, wantN: 1, wantErr: true},
		{name: "create 404 is a failure", item: bulkResponseItems{Create: notFound("k1")}, wantN: 1, wantErr: true},
		{name: "index 404 is a failure", item: bulkResponseItems{Index: notFound("k1")}, wantN: 1, wantErr: true},
		{
			name:  "delete 404 stays a success",
			item:  bulkResponseItems{Delete: &bulkResponseItem{ID: "k1", Status: http.StatusNotFound}},
			wantN: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items := []bulkResponseItems{bulkItemOK("k0"), tc.item, bulkItemOK("k2")}
			n, err := newTestDestination(t, items).Write(context.Background(), testRecords(3))

			require.Equal(t, tc.wantN, n)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "key_hash="+hashOf("k1")+" ")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDestination_Write_ErrorsDoNotContainKeysOrDocumentContent: Elasticsearch
// quotes the offending field value in a mapping error, and the document ID is
// the record key. Neither may reach the error, which can end up in the DLQ,
// the API and CLI output.
func TestDestination_Write_ErrorsDoNotContainKeysOrDocumentContent(t *testing.T) {
	items := []bulkResponseItems{{Update: &bulkResponseItem{
		ID:     "customer-SECRET-4711",
		Status: http.StatusBadRequest,
		Error: &bulkResponseItemError{
			Type:     "mapper_parsing_exception",
			Reason:   "failed to parse field [age] of type [integer] in document with id 'customer-SECRET-4711'. Preview of field's value: 'ssn-SECRET-123'",
			CausedBy: json.RawMessage(`{"reason":"For input string: \"ssn-SECRET-123\""}`),
		},
	}}}
	d := newTestDestination(t, items)

	_, err := d.Write(context.Background(), testRecords(1))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRET")
	require.Contains(t, err.Error(), "mapper_parsing_exception")
	require.Contains(t, err.Error(), "status 400")
}
