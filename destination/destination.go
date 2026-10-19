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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Default batching: the SDK buffers records into batches of sdk.batch.size
// records (default 1000) and flushes an incomplete batch after sdk.batch.delay
// (default 1s), so the destination sends one _bulk request per batch instead of
// one per record. Without batching every record is its own _bulk request and
// pays the full HTTP round trip and request setup per record; a batch of 1000
// amortizes both. BatchDelay bounds the latency of an incomplete batch on a
// slow pipeline: a record is flushed at most this long after it arrives.
//
// Batching never weakens Invariant 1: the SDK acks a record only after the
// _bulk request of its batch succeeded, and a batch that is not yet flushed is
// redelivered on restart (at-least-once, Invariant 3).
const (
	defaultBatchSize  = 1000
	defaultBatchDelay = 1 * time.Second
)

func NewDestination() sdk.Destination {
	// The batching defaults are set on the config before the SDK parses it, so
	// they become the defaults of sdk.batch.size and sdk.batch.delay (the SDK's
	// own default is 0, i.e. batching off: one _bulk request per record). A user
	// can still override them, or set sdk.batch.size to 0 to disable batching.
	d := &Destination{
		config: Config{
			DefaultDestinationMiddleware: sdk.DefaultDestinationMiddleware{
				DestinationWithBatch: sdk.DestinationWithBatch{
					BatchSize:  defaultBatchSize,
					BatchDelay: defaultBatchDelay,
				},
			},
		},
	}
	return sdk.DestinationWithMiddleware(d)
}

type Destination struct {
	sdk.UnimplementedDestination

	config       Config
	getIndexName IndexFn

	client client

	// schemas memoizes parsed payload schemas for renderDecimals.
	schemas parsedSchemas
}

//go:generate moq -out client_moq_test.go . client
type client = elasticsearch.Client

// GetClient returns the current Elasticsearch client.
func (d *Destination) GetClient() elasticsearch.Client {
	return d.client
}

// Config returns the destination configuration. The SDK parses the connector
// configuration into it, validates it (see Config.Validate) and then calls Open.
func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(ctx context.Context) (err error) {
	// Config.Validate already rejected an invalid index, so this only builds
	// the function.
	d.getIndexName, err = d.config.IndexFunction()
	if err != nil {
		return fmt.Errorf("invalid index name or index function: %w", err)
	}

	// Initialize Elasticsearch client
	d.client, err = elasticsearch.NewClient(d.config.Version, d.config)
	if err != nil {
		return fmt.Errorf("failed creating client: %w", err)
	}

	// Check the connection
	if err := d.client.Ping(ctx); err != nil {
		return fmt.Errorf("server cannot be pinged: %w", err)
	}

	return nil
}

// Write sends records to Elasticsearch as one bulk request.
//
// It returns the number of records at the start of the batch that
// Elasticsearch confirmed, together with the error that stopped the write. The
// SDK acks records[:n] and nacks records[n:], so records[n] is the first record
// that was NOT confirmed.
//
// Delivery is at-least-once. Bulk items are independent, so items after the
// first failure may already be indexed; they are nacked with the rest of the
// batch and sent again. Re-sending is safe for keyed records: they are written
// as updates with doc_as_upsert under the record key as the document ID, and
// deletes are idempotent. Records without a key are indexed with a generated
// ID, so a re-sent keyless record is stored twice.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	// todo return retries

	// Invariant 6: decimals go out as exact decimal text, not as the ratio
	// encoding/json makes of a *big.Rat (#171). A failure here sends nothing,
	// so nothing is acked.
	docs, err := d.renderDecimals(ctx, records)
	if err != nil {
		return 0, err
	}

	// Prepare request payload
	data, err := d.prepareBulkRequestPayload(docs)
	if err != nil {
		return 0, err
	}

	// Send the bulk request
	response, err := d.executeBulkRequest(ctx, data)
	if err != nil {
		return 0, err
	}

	// NB: The order of responses is the same as the order of requests
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
	items := response.Items
	if len(items) > len(records) {
		// The SDK slices batch[n:], so n must never exceed len(records).
		items = items[:len(records)]
	}
	for n, item := range items {
		// Detect operation result
		var itemResponse bulkResponseItem
		var operationType string

		switch {
		case item.Index != nil:
			itemResponse = *item.Index
			operationType = "index"

		case item.Create != nil:
			itemResponse = *item.Create
			operationType = "create"

		case item.Update != nil:
			itemResponse = *item.Update
			operationType = "update"

		case item.Delete != nil:
			itemResponse = *item.Delete
			operationType = "delete"

		default:
			// Invariant 1: an item we cannot read is not a confirmed write.
			return n, fmt.Errorf(
				"item %d: no index, create, update or delete details were found in Elasticsearch response",
				n,
			)
		}

		if itemResponse.Status >= 200 && itemResponse.Status < 300 {
			continue
		}
		// Invariant 1: a 404 confirms only a delete (the document is already
		// gone). For index/create/update it means nothing was stored, e.g. the
		// index is missing and auto-creation is disabled (#154).
		if itemResponse.Status == http.StatusNotFound && operationType == "delete" {
			continue
		}

		// Invariant 1: ack only after Elasticsearch confirms the write. Item n
		// failed, so exactly records[:n] are confirmed. Returning n+1 would make
		// the SDK ack the failed record and lose it (#153).
		if itemResponse.Error == nil {
			return n, fmt.Errorf(
				"item with key=%s %s failure: unknown error status: %d",
				itemResponse.ID,
				operationType,
				itemResponse.Status,
			)
		}

		return n, fmt.Errorf(
			"item with key=%s %s failure: [%s] %s: %s",
			itemResponse.ID,
			operationType,
			itemResponse.Error.Type,
			itemResponse.Error.Reason,
			itemResponse.Error.CausedBy,
		)
	}

	// Invariant 1: every record needs its own confirmation. A response with
	// fewer items than the request leaves the rest unconfirmed.
	if len(items) < len(records) {
		return len(items), fmt.Errorf(
			"bulk response contains %d items for %d records",
			len(response.Items),
			len(records),
		)
	}

	return len(records), nil
}

func (d *Destination) Teardown(context.Context) error {
	return nil // No close routine needed
}

// bulkLineEstimate is the conservative per-record byte estimate used to
// pre-size the bulk request buffer. It covers the action line plus the source
// envelope; the buffer grows further if payloads are larger. See
// prepareBulkRequestPayload.
const bulkLineEstimate = 128

// prepareBulkRequestPayload converts all pending operations into a valid Elasticsearch Bulk API request.
func (d *Destination) prepareBulkRequestPayload(records []opencdc.Record) (*bytes.Buffer, error) {
	data := &bytes.Buffer{}
	// Each record contributes an action line and a source line. Grow once with
	// a conservative per-record estimate so the buffer does not repeatedly
	// reallocate (and zero) as it grows to the final size.
	data.Grow(len(records) * bulkLineEstimate)

	for _, record := range records {
		index, err := d.getIndexName(record)
		if err != nil {
			return nil, err
		}

		var key string
		if record.Key != nil {
			key = string(record.Key.Bytes())
		}

		op := record.Operation
		if key == "" {
			op = opencdc.OperationCreate
		}
		switch {
		case key == "":
			if err := d.client.WriteCreateOperation(data, record, index); err != nil {
				return nil, fmt.Errorf("failed to prepare metadata: %w", err)
			}

		case op == opencdc.OperationSnapshot || op == opencdc.OperationCreate || op == opencdc.OperationUpdate:
			if err := d.client.WriteUpsertOperation(data, key, record, index); err != nil {
				return nil, fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
			}

		case op == opencdc.OperationDelete:
			if err := d.client.WriteDeleteOperation(data, key, index); err != nil {
				return nil, fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
			}

		default:
			return nil, fmt.Errorf("operation %v on record %v not supported", record.Operation, record.Key)
		}
	}

	return data, nil
}

// executeBulkRequest executes Bulk API request and parses the response.
func (d *Destination) executeBulkRequest(ctx context.Context, data *bytes.Buffer) (bulkResponse, error) {
	// Check if there is any job to do
	if data.Len() < 1 {
		sdk.Logger(ctx).Info().Msg("no operations to execute in bulk, skipping")

		return bulkResponse{}, nil
	}

	defer data.Reset()

	// Execute the request
	responseBody, err := d.client.Bulk(ctx, bytes.NewReader(data.Bytes()))
	if err != nil {
		return bulkResponse{}, fmt.Errorf("bulk request failure: %w", err)
	}

	// Get the response
	bodyContents, err := io.ReadAll(responseBody)
	if err != nil {
		_ = responseBody.Close()

		return bulkResponse{}, fmt.Errorf("bulk response failure: failed to read the result: %w", err)
	}

	if err := responseBody.Close(); err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: failed to read the result: %w", err)
	}

	// Read individual errors
	var response bulkResponse
	if err := json.Unmarshal(bodyContents, &response); err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: could not read the response: %w", err)
	}

	return response, nil
}
