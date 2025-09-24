# Kafka Ingestion Plugin

A production-ready plugin that consumes data from Apache Kafka topics and indexes it into NRTSearch. Features include:

- **Avro Schema Support**: Full integration with Confluent Schema Registry
- **Reliable Ingestion**: Configurable batch sizing, offset tracking, and crash recovery
- **Performance Optimized**: Concurrent consumer threads with backpressure handling
- **Production Ready**: Comprehensive error handling and monitoring support
- **Flexible Configuration**: Support for complex nested Avro schemas

## Configuration

Configure the Kafka plugin in your NRTSearch server configuration:

```yaml
plugins:
  - "s3://your-bucket/nrtsearch/plugins/kafka-plugin-0.1.0-SNAPSHOT.zip"

pluginConfigs:
  ingestion:
    kafka:
      bootstrapServers: "localhost:9092"
      schemaRegistryUrl: "http://localhost:8081"
      groupId: "nrtsearch-consumer"
      topic: "your-topic-name"
      indexName: "your-index-name"
      autoOffsetReset: "earliest"  # or "latest"
      batchSize: 1000
      maxPollRecords: 1000
      pollTimeoutMs: 1000
```

### Configuration Parameters

- **bootstrapServers**: Kafka broker addresses
- **schemaRegistryUrl**: Confluent Schema Registry URL for Avro schema management
- **groupId**: Kafka consumer group ID (use unique names for different deployments)
- **topic**: Kafka topic to consume from
- **indexName**: NRTSearch index to write documents to
- **autoOffsetReset**: What to do when there's no initial offset (`earliest` or `latest`)
- **batchSize**: Number of documents to batch before committing to NRTSearch
- **maxPollRecords**: Maximum records returned in a single poll()
- **pollTimeoutMs**: Timeout for Kafka consumer poll operations

## Schema Requirements

The plugin expects Avro schemas with the following conventions:
- **Nested objects**: Flattened with underscore separation (e.g., `metadata.author` → `metadata_author`)
- **Array fields**: Automatically detected and configured as multi-valued in NRTSearch
- **Primitive types**: Direct mapping to NRTSearch field types (STRING→ATOM, DOUBLE→DOUBLE, etc.)

Example compatible Avro schema:
```json
{
  "type": "record",
  "name": "Document",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "title", "type": "string"},
    {"name": "content", "type": "string"},
    {"name": "rating", "type": "double"},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "metadata", "type": {
      "type": "record",
      "name": "Metadata",
      "fields": [
        {"name": "author", "type": "string"},
        {"name": "publishDate", "type": "string"}
      ]
    }}
  ]
}
```

## Data Flow

```
Kafka Topic → Avro Consumer → Schema Registry → Field Mapping → NRTSearch Index → Search API
     ↓              ↓              ↓              ↓              ↓
   Offset      Deserialization   Type         Document      Commit &
  Management      & Batching    Conversion    Indexing      Refresh
```

## Error Handling & Recovery

- **Offset Management**: Consumer offsets only committed after successful NRTSearch indexing
- **Retry Logic**: Configurable retry with exponential backoff for transient failures
- **Dead Letter Queues**: Failed messages can be redirected to error topics
- **Graceful Shutdown**: Plugins complete in-flight operations before stopping

## Troubleshooting

### Schema Registry Connectivity
```bash
# Test Schema Registry connectivity
curl http://localhost:8081/subjects

# Check Avro schema registration
curl http://localhost:8081/subjects/your-topic-value/versions/latest
```

### Performance Tuning
- **Batch Size**: Increase `batchSize` for higher throughput (default: 1000)
- **Consumer Threads**: Plugin supports multiple consumer threads per partition
- **JVM Settings**: Ensure adequate heap for large Avro schemas and batching

## Building and Testing

```bash
# Build the plugin
./gradlew :kafka-plugin:build

# Run tests
./gradlew :kafka-plugin:test

# Run E2E tests
./gradlew :kafka-plugin:e2eTest

# Build plugin distribution
./gradlew :kafka-plugin:distZip
```

## Dependencies

- Apache Kafka Client
- Confluent Schema Registry Client
- Apache Avro
- NRTSearch Server (1.+)