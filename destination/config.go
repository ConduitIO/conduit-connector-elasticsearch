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
	"fmt"
	"math"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type IndexFn func(opencdc.Record) (string, error)

type Config struct {
	sdk.DefaultDestinationMiddleware

	// The version of the Elasticsearch service. One of: 5, 6, 7, 8.
	Version elasticsearch.Version `json:"version" validate:"required"`
	// The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).
	Host string `json:"host" validate:"required"`
	// The username for HTTP Basic Authentication.
	Username string `json:"username"`
	// The password for HTTP Basic Authentication.
	Password string `json:"password"`
	// Endpoint for the Elastic Service (https://elastic.co/cloud).
	CloudID string `json:"cloudID"`
	// Base64-encoded token for authorization; if set, overrides username/password and service token.
	APIKey string `json:"APIKey"`
	// Service token for authorization; if set, overrides username/password.
	ServiceToken string `json:"serviceToken"`
	// SHA256 hex fingerprint given by Elasticsearch on first launch.
	CertificateFingerprint string `json:"certificateFingerprint"`
	// The name of the index to write the data to.
	Index string `json:"index" default:"{{ index .Metadata \"opencdc.collection\" }}"`
	// The name of the index's type to write the data to.
	Type string `json:"type"`
	// Deprecated: use `sdk.batch.size`. The number of items stored in one bulk
	// request (the minimum value is `1`, the maximum value is `10 000`). This
	// config was never read in the write path, so its old default of 1000 did
	// nothing. It is now honored as a backward-compatible alias for
	// `sdk.batch.size` when explicitly set (non-zero): a pipeline that set it
	// keeps its intended batch size. It has no default; leave it unset to use
	// `sdk.batch.size` (default 1000).
	BulkSize uint64 `json:"bulkSize"`
	// The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255.
	Retries uint8 `json:"retries" default:"0"`
}

// Validate rejects, at configuration time as before the SDK v0.15 move, an
// index that is neither a static name nor a valid Go template and an unknown
// sdk.record.format, and runs the middleware
// validations it would otherwise shadow.
func (c *Config) Validate(ctx context.Context) error {
	if err := c.DefaultDestinationMiddleware.Validate(ctx); err != nil {
		return err
	}
	// DefaultDestinationMiddleware.Validate only finds Validate methods with a
	// value receiver, so it skips the record format check, and the SDK's Open
	// drops that error too (ConduitIO/conduit-connector-sdk#409).
	if err := c.DestinationWithRecordFormat.Validate(ctx); err != nil {
		return err
	}
	if err := c.resolveDeprecatedBulkSize(ctx); err != nil {
		return err
	}
	if _, err := c.IndexFunction(); err != nil {
		return fmt.Errorf("invalid index name or index function: %w", err)
	}
	return nil
}

// resolveDeprecatedBulkSize maps the deprecated bulkSize config onto
// sdk.batch.size when it is explicitly set. bulkSize predates destination
// batching and was never read in the write path, so its old default (1000) did
// nothing; it now has no default (0 means unset). An explicit value becomes the
// batch size for backward compatibility: a pipeline that set bulkSize keeps its
// intended bulk request size. sdk.batch.size is the canonical knob and applies
// whenever bulkSize is unset.
func (c *Config) resolveDeprecatedBulkSize(ctx context.Context) error {
	if c.BulkSize == 0 {
		return nil
	}
	if c.BulkSize > uint64(math.MaxInt) {
		return fmt.Errorf("bulkSize %d is too large to be used as a batch size", c.BulkSize)
	}
	c.BatchSize = int(c.BulkSize)
	sdk.Logger(ctx).Warn().
		Uint64("bulkSize", c.BulkSize).
		Msg("bulkSize is deprecated and will be removed in a future release; set sdk.batch.size instead")
	return nil
}

func (c Config) GetHost() string {
	return c.Host
}

func (c Config) GetUsername() string {
	return c.Username
}

func (c Config) GetPassword() string {
	return c.Password
}

func (c Config) GetCloudID() string {
	return c.CloudID
}

func (c Config) GetAPIKey() string {
	return c.APIKey
}

func (c Config) GetServiceToken() string {
	return c.ServiceToken
}

func (c Config) GetCertificateFingerprint() string {
	return c.CertificateFingerprint
}

func (c Config) GetType() string {
	return c.Type
}

// IndexFunction returns a function that determines the index for each record individually.
// The function might be returning a static index name.
// If the index is neither static nor a template, an error is returned.
func (c Config) IndexFunction() (f IndexFn, err error) {
	// Not a template, i.e. it's a static index name
	if !strings.Contains(c.Index, "{{") && !strings.Contains(c.Index, "}}") {
		return func(_ opencdc.Record) (string, error) {
			return c.Index, nil
		}, nil
	}

	// Try to parse the index
	t, err := template.New("index").Funcs(sprig.FuncMap()).Parse(c.Index)
	if err != nil {
		// The index is not a valid Go template.
		return nil, fmt.Errorf("index is neither a valid static index nor a valid Go template: %w", err)
	}

	// The index is a valid template, return IndexFn.
	var buf bytes.Buffer
	return func(r opencdc.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute index template: %w", err)
		}
		return buf.String(), nil
	}, nil
}
