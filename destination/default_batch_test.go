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

package destination_test

import (
	"context"
	"testing"
	"time"

	elasticsearch "github.com/conduitio/conduit-connector-elasticsearch"
	"github.com/conduitio/conduit-connector-elasticsearch/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/stretchr/testify/require"
)

// TestNewDestination_DefaultBatching is the regression test for the default
// batching: without an explicit sdk.batch.size the SDK used to hand the
// destination one record per Write, so every record became its own _bulk POST.
// The destination now batches by default; the user can still set
// sdk.batch.size to 0 to restore one _bulk POST per record. It also pins the
// deprecated bulkSize alias: an explicit bulkSize maps onto sdk.batch.size.
func TestNewDestination_DefaultBatching(t *testing.T) {
	params := elasticsearch.Connector.NewSpecification().DestinationParams

	t.Run("default", func(t *testing.T) {
		d := destination.NewDestination()
		require.NoError(t, sdk.Util.ParseConfig(context.Background(), baseConfig(), d.Config(), params))
		got := d.Config().(*destination.Config)
		require.Equal(t, 1000, got.BatchSize)
		require.Equal(t, time.Second, got.BatchDelay)
	})

	t.Run("explicit override", func(t *testing.T) {
		d := destination.NewDestination()
		cfg := baseConfig()
		cfg["sdk.batch.size"] = "50"
		cfg["sdk.batch.delay"] = "20ms"
		require.NoError(t, sdk.Util.ParseConfig(context.Background(), cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		require.Equal(t, 50, got.BatchSize)
		require.Equal(t, 20*time.Millisecond, got.BatchDelay)
	})

	t.Run("disable with zero", func(t *testing.T) {
		d := destination.NewDestination()
		cfg := baseConfig()
		cfg["sdk.batch.size"] = "0"
		cfg["sdk.batch.delay"] = "0"
		require.NoError(t, sdk.Util.ParseConfig(context.Background(), cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		require.Equal(t, 0, got.BatchSize)
		require.Equal(t, time.Duration(0), got.BatchDelay)
	})

	t.Run("bulkSize deprecated alias", func(t *testing.T) {
		d := destination.NewDestination()
		cfg := baseConfig()
		cfg["bulkSize"] = "500"
		require.NoError(t, sdk.Util.ParseConfig(context.Background(), cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		require.Equal(t, uint64(500), got.BulkSize)
		require.Equal(t, 500, got.BatchSize)
	})
}

func baseConfig() map[string]string {
	return map[string]string{
		"version": "8",
		"host":    "http://127.0.0.1:1",
		"index":   "idx",
	}
}
