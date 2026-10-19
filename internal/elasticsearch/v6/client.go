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

package v6

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch/jsonutil"

	"github.com/elastic/go-elasticsearch/v6"
)

func NewClient(cfg interface{}) (*Client, error) {
	configTyped, ok := cfg.(config)
	if !ok {
		return nil, errors.New("provided config object is invalid")
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{configTyped.GetHost()},
		Username:  configTyped.GetUsername(),
		Password:  configTyped.GetPassword(),
		CloudID:   configTyped.GetCloudID(),
		APIKey:    configTyped.GetAPIKey(),
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		es:  esClient,
		cfg: configTyped,
	}, nil
}

type Client struct {
	es  *elasticsearch.Client
	cfg config
}

// GetClient returns Elasticsearch v6 client.
func (c *Client) GetClient() *elasticsearch.Client {
	return c.es
}

func (c *Client) Ping(ctx context.Context) error {
	ping, err := c.es.Ping(c.es.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	if ping.IsError() {
		return fmt.Errorf("host ping failed: %s", ping.Status())
	}

	return nil
}

func (c *Client) Bulk(ctx context.Context, reader io.Reader) (io.ReadCloser, error) {
	result, err := c.es.Bulk(reader, c.es.Bulk.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if result.IsError() {
		bodyContents, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read the result: %w", err)
		}

		if err := result.Body.Close(); err != nil {
			return nil, fmt.Errorf("failed to read the result: %w", err)
		}

		var errorDetails ErrorResponse
		if err := json.Unmarshal(bodyContents, &errorDetails); err != nil {
			return nil, errors.New(result.Status())
		}

		return nil, fmt.Errorf("[%s] %s", errorDetails.Error.Type, errorDetails.Error.Reason)
	}

	return result.Body, nil
}

// WriteCreateOperation appends the index action and source lines for a
// keyless record to w as two newline-delimited JSON lines. Elasticsearch 6
// indexes new documents with the `index` action and requires a `_type`.
func (c *Client) WriteCreateOperation(w *bytes.Buffer, item opencdc.Record, index string) error {
	payload, err := preparePayload(&item)
	if err != nil {
		return err
	}

	w.WriteString(`{"index":{"_index":`)
	jsonutil.WriteString(w, index)
	w.WriteString(`,"_type":`)
	jsonutil.WriteString(w, c.cfg.GetType())
	w.WriteString("}}\n")
	w.Write(payload)
	w.WriteByte('\n')

	return nil
}

// WriteUpsertOperation appends the update action and source lines for a keyed
// record to w as two newline-delimited JSON lines.
func (c *Client) WriteUpsertOperation(w *bytes.Buffer, key string, item opencdc.Record, index string) error {
	payload, err := preparePayload(&item)
	if err != nil {
		return err
	}

	w.WriteString(`{"update":{"_id":`)
	jsonutil.WriteString(w, key)
	w.WriteString(`,"_index":`)
	jsonutil.WriteString(w, index)
	w.WriteString(`,"_type":`)
	jsonutil.WriteString(w, c.cfg.GetType())
	w.WriteString(`,"retry_on_conflict":3}}` + "\n" + `{"doc":`)
	w.Write(payload)
	w.WriteString(`,"doc_as_upsert":true}` + "\n")

	return nil
}

// WriteDeleteOperation appends the delete action line for a keyed record to w
// as one newline-delimited JSON line.
func (c *Client) WriteDeleteOperation(w *bytes.Buffer, key string, index string) error {
	w.WriteString(`{"delete":{"_id":`)
	jsonutil.WriteString(w, key)
	w.WriteString(`,"_index":`)
	jsonutil.WriteString(w, index)
	w.WriteString(`,"_type":`)
	jsonutil.WriteString(w, c.cfg.GetType())
	w.WriteString("}}\n")

	return nil
}

// preparePayload encodes Record's payload as JSON, ready to be embedded in a
// bulk action's source line.
//
// StructuredData is marshaled with encoding/json, which is already compact and
// HTML escaped. RawData is compacted so that invalid JSON is rejected before
// anything is sent (invariant 6: never silently mangle) and insignificant
// whitespace — including newlines that would break the newline-delimited bulk
// framing — is stripped. An absent payload is written as JSON null, the
// faithful encoding (invariant 3), so the record keeps flowing.
func preparePayload(item *opencdc.Record) ([]byte, error) {
	if item.Payload.After == nil {
		// Invariant 3: an absent payload still has to be written. JSON null is
		// the faithful encoding and keeps the record flowing (never skip).
		return []byte("null"), nil
	}

	switch itemPayload := item.Payload.After.(type) {
	case opencdc.StructuredData:
		return json.Marshal(itemPayload)

	default:
		payload := itemPayload.Bytes()
		if len(payload) == 0 {
			// Invariant 6: an empty RawData is an absent payload (the SDK no
			// longer invents an all-null record for it). Write JSON null rather
			// than emit an empty json.RawMessage, which fails to marshal and
			// drops the record.
			return []byte("null"), nil
		}
		// Re-marshal the raw bytes as a json.RawMessage: this validates the
		// JSON and strips insignificant whitespace (including newlines that
		// would break the newline-delimited framing) exactly as the previous
		// struct-based encoding did, so the emitted bytes are unchanged.
		compacted, err := json.Marshal(json.RawMessage(payload))
		if err != nil {
			return nil, fmt.Errorf("payload is not valid JSON: %w", err)
		}
		return compacted, nil
	}
}
