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

## S3A FileSystem Troubleshooting

The most common issue with Paimon S3A integration is the "No FileSystem for scheme: s3a" error. This section explains the root causes and solutions.

### Root Cause Analysis

**The Problem**: Apache Paimon and Hadoop have a service discovery bug where `S3AFileSystem` is not properly registered in `META-INF/services/org.apache.hadoop.fs.FileSystem` files.

**Affected Versions**:
- ❌ `paimon-s3:1.2.0` - Missing S3AFileSystem service registration
- ❌ `hadoop-aws:3.3.6` - Missing S3AFileSystem service registration
- ✅ `hadoop-aws:2.7.3, 2.8.3` - Have proper S3AFileSystem service registration

**Why It Works in Flink**: Flink deployments work because they use separate `hadoop-aws` JARs in the shared classloader, not shaded within individual applications.

### Architecture Comparison

| **Flink Environment (Standard Approach)**    | **NRTSearch Plugin (Our Approach)** |
|----------------------------------------------|--------------------------------------|
| Shared parent classloader                    | Isolated plugin classloader |
| `hadoop-aws-2.8.3` in `$FLINK_HOME/lib/`     | Everything shaded in plugin JAR |
| ServiceLoader works across classpath         | ServiceLoader isolated to plugin |
| External dependency management               | Self-contained plugin |
| **Pros**: Works with standard Hadoop JARs    | **Pros**: No external dependencies |
| **Cons**: Requires runtime environment setup | **Cons**: Must handle service registration manually |

### Our Solution

We implement a **dual-fix approach** that addresses both the service registration and classloader isolation issues:

1. **Service File Fix**: Manual registration of `S3AFileSystem` in plugin resources
2. **ClassLoader Context Fix**: Set thread context classloader before Paimon initialization

```java
// ClassLoader Context Fix in PaimonIngestor.java
ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

try {
    // Create catalog - now ServiceLoader can find S3AFileSystem
    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
    this.catalog = CatalogFactory.createCatalog(catalogContext);
} finally {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
}
```

### Debugging S3A Issues

**1. Verify Service Registration**:
```bash
# Extract your plugin JAR and check service file
jar tf your-plugin.jar | grep -E "META-INF/services.*FileSystem"
jar -xf your-plugin.jar META-INF/services/com.yelp.nrtsearch.plugins.paimon.shaded.hadoop.fs.FileSystem
cat META-INF/services/com.yelp.nrtsearch.plugins.paimon.shaded.hadoop.fs.FileSystem

# Should contain:
# com.yelp.nrtsearch.plugins.paimon.shaded.hadoop.fs.s3a.S3AFileSystem
```

**2. Verify S3AFileSystem Class Exists**:
```bash
jar tf your-plugin.jar | grep S3AFileSystem.class
# Should show: com/yelp/nrtsearch/plugins/paimon/shaded/hadoop/fs/s3a/S3AFileSystem.class
```

**3. Test S3A Configuration**:
```bash
# Verify S3A credentials and bucket access
aws s3 ls s3a://your-bucket-name/

# Check IAM permissions for your service role
aws sts get-caller-identity
```

### Alternative Solutions

**Option 1: Use s3:// instead of s3a://**:
```yaml
# Change warehouse path from s3a:// to s3://
paimonConfig:
  warehouse.path: "s3://your-bucket/warehouse"  # Instead of s3a://
```

**Option 2: Downgrade to Working Hadoop Version**:
```groovy
// Replace hadoop-aws:3.3.6 with version that has service registration
implementation 'org.apache.hadoop:hadoop-common:2.8.3'
implementation 'org.apache.hadoop:hadoop-client:2.8.3'
implementation 'org.apache.hadoop:hadoop-aws:2.8.3'
```

### Why Our Approach is Better

1. **Self-Contained**: No dependency on runtime environment Hadoop JARs
2. **Version Independent**: Works regardless of hadoop-aws version bugs
3. **Plugin Architecture**: Designed for isolated execution environments
4. **More Robust**: Explicit control over service registration

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