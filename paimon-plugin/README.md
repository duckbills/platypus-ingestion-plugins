# Paimon Ingestion Plugin

Apache Paimon ingestion plugin for nrtsearch with Dynamic Shared Queue architecture to handle workload skew in dynamic bucket tables.

## Features

- **Dynamic Shared Queue Architecture**: Coordinator-worker pattern that adapts to workload skew in dynamic bucket tables
- **Incremental Processing**: Uses Paimon's checkpoint/restore mechanism for efficient incremental data processing
- **Ordering Guarantees**: Maintains ordering for same keys by sorting splits by sequence number within buckets
- **Configurable Parallelism**: Adjustable worker threads for optimal throughput
- **Field Mapping**: Flexible mapping between Paimon fields and nrtsearch index fields

## Configuration

### Required Configuration

```yaml
ingestionPluginConfigs:
  paimon:
    table.path: "database_name.table_name"          # Paimon table path
    target.index.name: "nrtsearch_index_name"       # Target nrtsearch index
    warehouse.path: "/path/to/paimon/warehouse"     # Paimon warehouse path (filesystem or S3)
```

### S3 Configuration

The plugin supports both filesystem and S3-based Paimon warehouses with automatic S3A configuration:

#### Production S3 Configuration (IAM Roles)
```yaml
ingestionPluginConfigs:
  paimon:
    warehouse.path: "s3a://prod-bucket/warehouse/"  # S3A warehouse path
    table.path: "production.events"
    target.index.name: "events-index"
    # No explicit S3 config - uses IAM roles automatically
```

#### Test/Local S3 Configuration (Explicit Credentials)
```yaml
ingestionPluginConfigs:
  paimon:
    warehouse.path: "s3a://test-bucket/warehouse/"  # S3A warehouse path
    table.path: "test.events"
    target.index.name: "test-events-index"
    s3:
      endpoint: "http://127.0.0.1:8011"             # S3Mock endpoint for testing
      s3-access-key: "test-key"
      s3-secret-key: "test-secret"
      path.style.access: "true"                     # Required for S3Mock
```

#### Filesystem Configuration
```yaml
ingestionPluginConfigs:
  paimon:
    warehouse.path: "file:///tmp/paimon-warehouse/" # Local filesystem
    table.path: "test_db.documents"
    target.index.name: "local-index"
```

### Optional Configuration

```yaml
ingestionPluginConfigs:
  paimon:
    # Worker configuration
    worker.threads: 4                               # Number of worker threads (default: 4)
    batch.size: 1000                               # Documents per batch (default: 1000)

    # Polling configuration
    poll.timeout.ms: 1000                          # Worker poll timeout (default: 1000)
    scan.interval.ms: 30000                        # Coordinator scan interval (default: 30000)
    queue.capacity: 10000                          # Work queue capacity (default: 10000)

    # Field mapping (optional)
    field.mapping:
      paimon_field_name: "nrtsearch_field_name"
      user_id: "userId"
```

### S3A Configuration Details

The plugin automatically configures S3A filesystem when `warehouse.path` starts with `s3a://`:

- **Universal Settings**: S3A implementation, connection pooling, performance optimizations
- **Credential Selection**:
  - **With endpoint**: Uses SimpleAWSCredentialsProvider (test/local environments)
  - **Without endpoint**: Uses WebIdentityTokenCredentialsProvider + DefaultAWSCredentialsProviderChain (production IAM roles)
- **Performance Tuning**: 256 max connections, 128 threads, 64MB block size

## Architecture

### Dynamic Shared Queue

1. **Coordinator Thread**: 
   - Scans Paimon table for new splits since last checkpoint
   - Groups splits by bucket for ordering guarantees
   - Sorts splits within buckets by sequence number
   - Distributes work packages to shared queue

2. **Worker Threads**: 
   - Poll work packages from shared queue
   - Process buckets sequentially to maintain ordering
   - Convert Paimon InternalRow to nrtsearch documents
   - Index documents in configurable batches

### Ordering Guarantees

- **Bucket-level Ordering**: Similar to Kafka partition ordering
- **Sequence Number Sorting**: Splits within each bucket are sorted by minimum sequence number
- **Sequential Processing**: Each bucket is processed by a single worker thread sequentially

## TODO

See [TODO.md](TODO.md) for current development items and planned improvements.

## Building and Testing

```bash
# Build the plugin
./gradlew :paimon-plugin:build

# Run tests
./gradlew :paimon-plugin:test
```

## Dependencies

- Apache Paimon 1.2.0
- Hadoop Client 3.3.6
- nrtsearch server (1.+)