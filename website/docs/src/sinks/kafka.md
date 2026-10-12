# Apache Kafka sink

**Status: Available** for plaintext brokers. The `kafka` kind is a batch sink that publishes one dataset's events to Apache Kafka topics: one record per event, `acks=all`, JSON encoded, with the event id as the record key and in an `x-thadam-event-id` header. TLS and SASL are **In progress**: implemented and unit-tested, not yet exercised against a broker.

| Capability | Status |
| --- | --- |
| Produce one record per event to existing topics, `acks=all` | Available |
| JSON envelope, event-id key and header | Available |
| Delivery class `at_least_once`, reported at `open` | Available |
| Per-event quarantine: oversized events, unrenderable templates, topic names the broker refuses | Available |
| Fatal classification: missing topic | Available |
| Compression `none`, `gzip`, `snappy` | Available |
| Fatal classification: authorization and SASL failures | In progress |
| TLS with a CA file and a client certificate | In progress |
| SASL `plain`, `scram-sha-256`, `scram-sha-512` | In progress |

## How it works

1. The runtime forms batches on WAL frame boundaries and hands each batch to the sink with a stable batch id.
2. The sink renders each event's topic from the `topic` template and its key from the `key` template (default: the event id), encodes the event as one JSON document, and groups the records by (topic, partition). The partition is chosen the way the Java client's default partitioner chooses it (murmur2 of the key, modulo the partition count).
3. Each group is one produce request with `acks=all`. At most `max_in_flight` requests are unanswered at any moment. `write` returns when every record is acknowledged or individually rejected.
4. Topics must already exist. The sink never creates them: a missing topic is a fatal configuration error, whatever the broker's auto-creation setting.

## Configuration

```yaml
sinks:
  - id: kafka-out
    kind: kafka
    config:
      bootstrap: [kafka-1.internal:9092, kafka-2.internal:9092]
      topic: telemetry.{dataset}.{robot}   # must already exist
      key: "{robot}"                       # default: the event id (hex)
      tls:                                 # setting this block enables TLS
        ca_file: /etc/thadam/ca.pem         #   replaces the platform roots
        client_cert: /etc/thadam/cert.pem   #   with client_key: mutual TLS
        client_key: /etc/thadam/key.pem
      sasl:                                # plain requires tls
        mechanism: scram-sha-512           #   plain | scram-sha-256 | scram-sha-512
        user: thadam
        password: "${file:/run/secrets/kafka_password}"
      acks: all                            # the only value (default)
      compression: gzip                    # none (default) | gzip | snappy
      request_timeout: 10s                 # connect, metadata, and produce (default)
      max_in_flight: 256                   # produce requests awaiting a response (default)
      max_message_bytes: 1048576           # per-event size limit (default)
      datasets: [imu]                      # exactly one dataset per sink entry
```

Notes:

- `bootstrap` entries are `host:port`. Unknown keys and malformed values are refused when the pipeline is built at startup (`thadam run`); `thadam validate` checks the pipeline's structure and kind names.
- `topic` templates interpolate `{dataset}` and identity level names as whole dot-separated tokens: `telemetry.{dataset}.{robot}` is valid, `pre-{robot}` is refused. A rendered topic must use only `[a-zA-Z0-9._-]` and be at most 249 bytes; an event that cannot be rendered is rejected individually.
- `key` may place placeholders anywhere, and also accepts `{event_id}`. Non-`dataset` placeholders must be identity levels of the selected dataset; a typo fails when the pipeline is built, not per event.
- Secrets are plain strings **already resolved** by `${env:}`/`${file:}` interpolation ([secrets](../configuration/index.md#secrets)). The sink wraps the SASL password in a type whose `Debug` output is redacted, and it never logs secret values.
- `sasl` with `plain` is refused at build unless `tls` is set: PLAIN sends the password in clear text.
- A `kind: kafka` sink publishes exactly one dataset; add one sink entry per dataset.

## The JSON envelope

The document is identical to the [NATS JetStream sink's](nats.md#the-json-envelope): the `_`-prefixed lineage fields, the identity levels, then the record fields, with later regions never overwriting lineage. `_event_id` is the same hex string as the record key and the `x-thadam-event-id` header.

## Delivery guarantees

- **Reported class: `at_least_once`.** The client has no idempotent or transactional producer (its produce requests carry producer id `-1`), so a batch retried after an unknown outcome can land twice. The runtime retries whole batches. Consumers that must not see duplicates dedupe on the `x-thadam-event-id` header or the `_event_id` field.
- **Transient (retried).** Request timeouts, a broker that does not answer within `request_timeout`, dropped connections, leaders moving, quota throttling, and unknown broker error codes.
- **Fatal (the component is marked `Failed`).** A missing topic, SASL authentication failures, authorization denials for the topic or cluster, a compression codec or message format the broker refuses, and bad credentials at connect.
- **Per-event (quarantine).** An event over `max_message_bytes` (checked before produce), an event the broker refuses for its size (the group is re-produced one record at a time, so only the refused record is rejected), a topic name the broker refuses, and a template that cannot be rendered. The rest of the batch proceeds.

## Limits

- **At-least-once only.** No idempotent or transactional producer exists in the client. Duplicates after an outage or a stalled produce are expected; the duplicate count is not bounded.
- **`acks=all` only.** `acks` values other than `all` are refused.
- **Topics are operator-provisioned.** Partition counts are read from metadata when a topic is first written and cached for the life of the sink.
- **Auto-creation can still happen after a deletion.** The client asks for auto-creation on its own leader lookups, so a topic deleted while the sink runs can be re-created if the broker auto-creates. Keep `auto.create.topics.enable` off on production brokers.
- **Compression.** `none`, `gzip`, and `snappy` only. `lz4` and `zstd` bind C libraries and are not offered (ADR-0020).
- **TLS and SASL are not yet tested against a broker.** Certificate loading and configuration are unit-tested; the Docker tests run plaintext only.
- **Broker versions.** The Docker tests run against `apache/kafka` 4.3.1. Other broker versions are not tested.
- **One dataset per sink entry**, the same restriction as the NATS sink (ADR-0002 open work).

## Integration tests

Docker-gated tests run the sink against a real single-node KRaft broker (`apache/kafka:4.3.1`, pinned by tag, public Docker Hub only; re-pin with `THADAM_TEST_KAFKA_IMAGE`):

```text
cargo nextest run -p thadam-kafka --run-ignored only -E 'binary(docker_kafka) | binary(conformance)'
```

They cover publish-and-consume-back (topic routing, envelope, key and header), gzip and snappy batches read back intact, a broker stop and start between batches with the retry landing at least once, a produce that stalls with the broker frozen (a transient timeout, then a retry), oversized events rejected by the broker's own size limit and by the client's limit with the neighbours published, a missing topic fatal with auto-creation off, the sink never creating a missing topic with auto-creation on, and the `thadam-testkit` batch-sink conformance suite. Authorization, SASL, and TLS failures are not in the Docker set yet. The broker's heap is capped and the group runs two brokers at a time.

Each test starts its own container (labelled `thadam-kafka-test`) and removes it when the test ends, including on panic. To clean up after a killed run: `docker rm -f $(docker ps -aq --filter label=thadam-kafka-test)`. Set `RUST_LOG=debug` for sink and client logs during a run.
