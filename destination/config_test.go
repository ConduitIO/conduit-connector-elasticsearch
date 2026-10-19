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
	"context"
	"math"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestConfig_Getters(t *testing.T) {
	fakerInstance := faker.New()

	var (
		host                   = fakerInstance.Internet().URL()
		username               = fakerInstance.Internet().Email()
		password               = fakerInstance.Internet().Password()
		cloudID                = fakerInstance.RandomStringWithLength(32)
		apiKey                 = fakerInstance.RandomStringWithLength(32)
		serviceToken           = fakerInstance.RandomStringWithLength(32)
		certificateFingerprint = fakerInstance.Hash().SHA256()
		indexName              = fakerInstance.Lorem().Word()
		indexType              = fakerInstance.Lorem().Word()
	)

	config := Config{
		Host:                   host,
		Username:               username,
		Password:               password,
		CloudID:                cloudID,
		APIKey:                 apiKey,
		ServiceToken:           serviceToken,
		CertificateFingerprint: certificateFingerprint,
		Index:                  indexName,
		Type:                   indexType,
	}

	require.Equal(t, host, config.GetHost())
	require.Equal(t, username, config.GetUsername())
	require.Equal(t, password, config.GetPassword())
	require.Equal(t, cloudID, config.GetCloudID())
	require.Equal(t, apiKey, config.GetAPIKey())
	require.Equal(t, serviceToken, config.GetServiceToken())
	require.Equal(t, certificateFingerprint, config.GetCertificateFingerprint())
	require.Equal(t, indexType, config.GetType())
}

// TestConfig_resolveDeprecatedBulkSize covers the deprecated bulkSize alias:
// unset it leaves sdk.batch.size alone, an explicit value becomes the batch
// size, and a value that does not fit in an int is rejected rather than wrapped
// (int is 32-bit on linux/386, so this guard is reachable there).
func TestConfig_resolveDeprecatedBulkSize(t *testing.T) {
	t.Run("unset leaves batch size", func(t *testing.T) {
		c := &Config{
			DefaultDestinationMiddleware: sdk.DefaultDestinationMiddleware{
				DestinationWithBatch: sdk.DestinationWithBatch{BatchSize: 1000},
			},
		}
		require.NoError(t, c.resolveDeprecatedBulkSize(context.Background()))
		require.Equal(t, 1000, c.BatchSize)
	})

	t.Run("set maps to batch size", func(t *testing.T) {
		c := &Config{BulkSize: 500}
		require.NoError(t, c.resolveDeprecatedBulkSize(context.Background()))
		require.Equal(t, 500, c.BatchSize)
	})

	t.Run("too large is rejected", func(t *testing.T) {
		c := &Config{BulkSize: math.MaxUint64}
		err := c.resolveDeprecatedBulkSize(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "too large to be used as a batch size")
	})
}

func TestConfig_IndexFunction(t *testing.T) {
	t.Run("static index name", func(t *testing.T) {
		config := Config{
			Index: "test-index",
		}

		indexFn, err := config.IndexFunction()
		require.NoError(t, err)
		require.NotNil(t, indexFn)

		index, err := indexFn(opencdc.Record{})
		require.NoError(t, err)
		require.Equal(t, "test-index", index)

		record := sdk.SourceUtil{}.NewRecordCreate(
			nil,
			map[string]string{"opencdc.collection": "other-index"},
			nil,
			nil,
		)
		index, err = indexFn(record)
		require.NoError(t, err)
		require.Equal(t, "test-index", index)
	})

	t.Run("template with metadata", func(t *testing.T) {
		config := Config{
			Index: "{{ index .Metadata \"opencdc.collection\" }}",
		}

		indexFn, err := config.IndexFunction()
		require.NoError(t, err)
		require.NotNil(t, indexFn)

		record := sdk.SourceUtil{}.NewRecordCreate(
			nil,
			map[string]string{"opencdc.collection": "dynamic-index"},
			nil,
			nil,
		)
		index, err := indexFn(record)
		require.NoError(t, err)
		require.Equal(t, "dynamic-index", index)

		record = sdk.SourceUtil{}.NewRecordCreate(nil, nil, nil, nil)
		index, err = indexFn(record)
		require.NoError(t, err)
		require.Equal(t, "", index)
	})

	t.Run("invalid template syntax", func(t *testing.T) {
		config := Config{
			Index: "{{ invalid syntax }}",
		}

		indexFn, err := config.IndexFunction()
		require.Error(t, err)
		require.Nil(t, indexFn)
		require.Contains(t, err.Error(), "index is neither a valid static index nor a valid Go template")
	})

	t.Run("template with invalid function", func(t *testing.T) {
		config := Config{
			Index: "{{ .InvalidFunction }}",
		}

		indexFn, err := config.IndexFunction()
		require.NoError(t, err)
		require.NotNil(t, indexFn)

		record := sdk.SourceUtil{}.NewRecordCreate(nil, nil, nil, nil)
		_, err = indexFn(record)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to execute index template")
	})
}
