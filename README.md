# Conduit Connector Elasticsearch

## General
The Elasticsearch plugin is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It currently provides only destination Elasticsearch connector, allowing for using it as a destination in a Conduit pipeline.

## How to build it
Run `make`.

# Destination

The Destination connector stores data in given index.
When Record has Key value set, then it is used as a Document ID.
Moreover, when Record has `action` entry in the Metadata, then action specified there is respected. Supported actions:
- `insert` when Record.Key is missing: stores a new Document without ID.
- `update`: stores or updates (upsert) a Document with ID. Default case when `action` is not set but Record.Key is set.
- `delete`: deletes a Document by its Record.Key.

For any other action a warning entry is added to log and Record is skipped.

## Configuration Options

| name                     | description                                                                                                                                                                                                                                      | required                                             | default  |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|----------|
| `version`                | The version of the Elasticsearch service. One of: `5`, `6`, `7`, `8`.                                                                                                                                                                            | `true`                                               |          |
| `host`                   | The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).                                                                                                                                                                                   | `true`                                               |          |
| `username`               | [v: 5, 6, 7, 8] The username for HTTP Basic Authentication.                                                                                                                                                                                      | `false`                                              |          |
| `password`               | [v: 5, 6, 7, 8] The password for HTTP Basic Authentication.                                                                                                                                                                                      | `true` when username was provided, `false` otherwise |          |
| `cloudId`                | [v: 6, 7, 8] Endpoint for the Elastic Service (https://elastic.co/cloud).                                                                                                                                                                        | `false`                                              |          |
| `apiKey`                 | [v: 6, 7, 8] Base64-encoded token for authorization; if set, overrides username/password and service token.                                                                                                                                      | `false`                                              |          |
| `serviceToken`           | [v: 7, 8] Service token for authorization; if set, overrides username/password.                                                                                                                                                                  | `false`                                              |          |
| `certificateFingerprint` | [v: 7, 8] SHA256 hex fingerprint given by Elasticsearch on first launch.                                                                                                                                                                         | `false`                                              |          |
| `index`                  | Index name. It can contain a Go template that will be executed for each record to determine the index. By default, the index is the value of the opencdc.collection metadata field.                                                             | `false`                                               | {{ index .Metadata \"opencdc.collection\" }} |
| `type`                   | [v: 5, 6] The name of the index's type to write the data to.                                                                                                                                                                                     | `true` for versions: `5` and `6`, `false` otherwise  |          |
| `bulkSize`               | Deprecated: use `sdk.batch.size`. The number of items stored in one bulk request (min `1`, max `10000`). It was never read in the write path; it is now honored as an alias for `sdk.batch.size` when explicitly set.                            | `false`                                              |          |
| `sdk.batch.size`         | Maximum size of batch before it gets written to the destination. Default `1000`: the destination sends one bulk request per batch of up to 1000 records. Set to `0` to disable batching (one bulk request per record).                            | `false`                                              | `"1000"` |
| `sdk.batch.delay`        | Maximum delay before an incomplete batch is written to the destination. Default `1s`.                                                                                                                                                               | `false`                                              | `"1s"`   |
| `retries`                | The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255`. Note that the higher value, the longer it may take to process retries, as a result, ingest next operations. | `false`                                              | `"0"`    |


## Delivery semantics

The destination is at-least-once. The SDK buffers records into batches of `sdk.batch.size` records (default 1000), flushed after `sdk.batch.delay` (default 1s) when a batch is still incomplete, and the destination sends each batch as one bulk request. A record is acknowledged only after Elasticsearch confirms its item. In earlier releases batching was off by default (`sdk.batch.size: 0`), so every record became its own bulk request; set `sdk.batch.size` to 0 to keep that behavior.

- When an item fails, the write stops at that record: records before it are acknowledged, the failed record and every record after it are not. Items after the failed one may already be stored in Elasticsearch.
- Records with a key are written as upserts by document ID, so sending them again overwrites the same document. Deletes are idempotent.
- Records without a key use `create` with a generated ID, so a replay stores them twice.
- With a dead-letter queue configured, one rejected item sends the whole tail of the batch (`batch[n:]`) to the DLQ, each record carrying the error of record `n`, including records Elasticsearch did store.
- Each of those tail records counts against `dlq.window-nack-threshold`.
- Routing is not supported. If documents were written with routing that the connector's delete does not send (by another writer, through a routed alias, or when deleting through the base index), Elasticsearch answers the delete with 404 `not_found` from the wrong shard. A 404 on a delete is treated as success, so the stale document stays.

## Avro-encoded records

When a record carries a key or payload schema (`opencdc.key.schema.*` / `opencdc.payload.schema.*` metadata), the SDK decodes it before the write (`sdk.schema.extract.key.enabled` and `sdk.schema.extract.payload.enabled`, both `true` by default) and the document is built from the decoded value. Decoding uses `github.com/iskorotkov/avro/v2` through conduit-commons; earlier releases used the archived `github.com/hamba/avro/v2`.

- Every empty Avro array, at any depth (a field, inside a record, a union, a map value or another array), is written as `[]`. Releases up to v0.4.1 wrote it as `null`, which could not be told apart from the `null` branch of a `["null", array]` union.
- Avro decimals (`bytes` or `fixed` with `logicalType: decimal`) are written as a JSON string of exact decimal text, with as many fractional digits as the schema's scale: `"-123.456"` at scale 3, `"1.50"` at scale 2, `"100"` at scale 0. This applies wherever the decimal sits: a top-level field, a nested record, an array, a map or a union. Some union values are still wrapped as `{"<branch>": value}` when they reach the connector (a union with a record, enum, fixed, map or array branch, outside the positions where conduit-commons unwraps them). The decimal inside gets that branch's scale, and the wrapper still reaches Elasticsearch as an object (#173). Map keys are data: a key that happens to equal a type name is never read as a wrapper. Releases up to v0.4.1 wrote the ratio of the decoded value instead (`"-15432/125"`, #171); see [Upgrading](#upgrading-avro-decimals).
  - A decimal is never written as a JSON number. Elasticsearch maps a JSON number it has not seen before as `float`, which keeps about 7 significant digits.
  - When the scale cannot be read (the record has no payload schema, or a union value no longer says which of two branches it came from), the text has the fewest digits that represent the value exactly (`"1.5"`).
  - When a record carries a decimal and its payload schema cannot be fetched, or does not parse on its own (it refers to a named type that only another schema defines), the write fails: the batch is not sent and no record in it is acknowledged. The SDK does not retry the write. With the default DLQ settings the pipeline stops, and records are redelivered when it is restarted; with `dlq.window-nack-threshold` above 0 the whole batch goes to the DLQ. A schema that does not parse on its own fails every time until it is fixed.
  - A decimal that has no exact decimal text fails the write the same way. A decoded Avro decimal always has exact text.
  - The record key is not changed. It becomes the document ID, so a decimal key is still printed as before (`{"id":"3/2"}`). Changing it would move existing documents to new IDs.

### Mapping decimal fields

The string goes into whatever mapping the index has. Choose it by what the field is used for:

| Mapping | What you get | Precision |
| --- | --- | --- |
| `keyword` | Exact text in `_source` and in the index. Term queries match the scale-padded text (`"1.50"`, not `"1.5"`). Range queries compare as strings. | Exact |
| `scaled_float` with `scaling_factor` = 10^scale | Numeric range queries, sorting and aggregations. Elasticsearch coerces the string to a number. | Stored as a long. Exact only while the value parses as a double without rounding, about 15 significant digits. |
| `double` | Numeric queries | About 15-17 significant digits |
| none (dynamic mapping) | `text` with a `keyword` subfield (`<field>.keyword`), because the value is a string. Numeric detection is off by default. | Exact in `_source` and `.keyword` |

`_source` always holds the exact string, whatever the mapping. To query a decimal both exactly and numerically, map it with `keyword` and add a `scaled_float` multi-field.

Index templates and dynamic templates see the rendered value: a JSON string such as `"1.50"`, so a dynamic template with `match_mapping_type: string` applies to decimal fields.

### Upgrading: Avro decimals

Documents written by earlier releases hold the ratio string (`"-15432/125"`). New writes and updates hold decimal text (`"-123.456"`). An upsert replaces the field, so a document becomes consistent once its record is written again.

- An index with dynamic mapping keeps the field as `text`/`keyword`, so old and new values coexist. Queries that matched the ratio text stop matching updated documents.
- An index that maps the field as a number (`scaled_float`, `double`) rejected every record carrying a decimal before. Those records now succeed.
- To make existing documents consistent, re-snapshot the source or reindex the affected indexes.
- Record keys are not changed, so document IDs stay the same: a decimal key is still written as before (`{"id":"3/2"}`), and upserts and deletes keep finding existing documents.

# Source
ElasticSearch source connector allows you to move data from multiple Elasticsearch indexes with the specified `host` and `indexes`. It uses elasticsearch search api to pull data from indexes. Upon starting it pulls batches of data from indexes, once all the data is retrieved, it then polls the search api to pull data at regular intervals. 

## Configuration Options
| name                     | description                                                                                                                                                                                                                                      | required                                             | default  |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|----------|
| `version`                | The version of the Elasticsearch service. Currently supports version `8`.                                                                                                                                                                           | `true`                                               |          |
| `host`                   | The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).                                                                                                                                                                                   | `true`                                               |          |
| `username`               | [v: 5, 6, 7, 8] The username for HTTP Basic Authentication.                                                                                                                                                                                      | `false`                                              |          |
| `password`               | [v: 5, 6, 7, 8] The password for HTTP Basic Authentication.                                                                                                                                                                                      | `true` when username was provided, `false` otherwise |          |
| `cloudId`                | [v: 6, 7, 8] Endpoint for the Elastic Service (https://elastic.co/cloud).                                                                                                                                                                        | `false`                                              |          |
| `apiKey`                 | [v: 6, 7, 8] Base64-encoded token for authorization; if set, overrides username/password and service token.                                                                                                                                      | `false`                                              |          |
| `serviceToken`           | [v: 7, 8] Service token for authorization; if set, overrides username/password.                                                                                                                                                                  | `false`                                              |          |
| `certificateFingerprint` | [v: 7, 8] SHA256 hex fingerprint given by Elasticsearch on first launch.                                                                                                                                                                         | `false`                                              |          |
| `indexes.*.sortBy`                  | The sortby field for each index to be used by elasticsearch search api.(A field must be specified for v5, v6 as it does not support sorting using the default `_seq_no`)                                                                                                                                                                                                       | `false`                                               |    `_seq_no`      |
| `indexes.*.sortOrder`                  | The sortOrder (asc or desc) for each index to be used by elasticsearch search api.                                                                                                                                                                                                      | `false`                                               |   `asc`       |
| `batchSize`               | The number of items to fetch from an index. The minimum value is `1`, maximum value is `10000`.                                                          | `false`                                               | `"1000"` |
| `pollingPeriod`                | The duration for polling the search api for fetching new records. | `false`                                               | `"5s"` |
| `retries`                | The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255`. Note that the higher value, the longer it may take to process retries, as a result, ingest next operations. | `false`                                               | `"0"` |

The record key is `{"id": "<document id>"}` as structured data. `sdk.schema.extract.key.enabled` defaults to `false` for this source: up to v0.4.1 the SDK never ran key schema extraction for it, so keys were never Avro-encoded, and the default keeps that. Set it to `true` to have the key encoded with an Avro schema registered under `<index>.key`.

# Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command will handle starting and stopping docker containers for you.

The integration tests connect to `http://127.0.0.1:9200` unless `ELASTICSEARCH_HOST` is set. To run them against a container on another port, start it with `ELASTICSEARCH_PORT=<port>` (the compose files publish that port) and set `ELASTICSEARCH_HOST=http://127.0.0.1:<port>`.

# Local development

If you like to monitor Elasticsearch index with visual editor, there is [Kibana](https://www.elastic.co/kibana/) configuration created for local development that you can run with Elasticsearch Docker container.

```shell
docker-compose -f test/docker-compose.v8.overrides.yml -f test/docker-compose.v8.yml -p test-v8 up -d
```

Don't forget to include it when finishing your work:

```shell
docker-compose -f test/docker-compose.v8.overrides.yml -f test/docker-compose.v8.yml down
```

# References

- https://github.com/elastic/go-elasticsearch
- https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html
- https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=715ebf4a-148c-44ad-8f64-6cc5780d34ae)
