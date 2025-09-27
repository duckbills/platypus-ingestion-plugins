# Paimon Ingestion Plugin

Apache Paimon ingestion plugin for nrtsearch with Dynamic Shared Queue architecture to handle workload skew in dynamic bucket tables.

## Features

- **Dynamic Shared Queue Architecture**: Coordinator-worker pattern that adapts to workload skew in dynamic bucket tables
- **Incremental Processing**: Uses Paimon's checkpoint/restore mechanism for efficient incremental data processing
- **Ordering Guarantees**: Maintains ordering for same keys by sorting splits by sequence number within buckets
- **Configurable Parallelism**: Adjustable worker threads for optimal throughput
- **Field Mapping**: Flexible mapping between Paimon fields and nrtsearch index fields
- **Field Dropping**: Drop unwanted fields by prefix to reduce index size and processing overhead
- **ID-Based Sharding**: Distributed processing across multiple service instances using modulo arithmetic on primary keys

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
    database.name: "production"                     # Database name (can contain dots)
    table.name: "events"                           # Table name (can contain dots)
    target.index.name: "events-index"
    # No explicit S3 config - uses IAM roles automatically
```

#### Test/Local S3 Configuration (Explicit Credentials)
```yaml
ingestionPluginConfigs:
  paimon:
    warehouse.path: "s3a://test-bucket/warehouse/"  # S3A warehouse path
    database.name: "test"                          # Database name
    table.name: "events"                           # Table name
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
    database.name: "test_db"                       # Database name
    table.name: "documents"                        # Table name
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

    # Field dropping (optional) - drop fields by prefix
    field.drop.prefixes:
      - "__internal__"
      - "__debug_"
      - "_temp_"

    # ID-based sharding (optional) - for distributed processing
    id_sharding_max: 30                            # Total number of shards
```

### S3A Configuration Details

The plugin automatically configures S3A filesystem when `warehouse.path` starts with `s3a://`:

- **Universal Settings**: S3A implementation, connection pooling, performance optimizations
- **Credential Provider Chain**:
  - **With endpoint**: Uses SimpleAWSCredentialsProvider (test/local environments)
  - **Without endpoint**: Uses DefaultAWSCredentialsProviderChain (production - supports IAM roles, aws-okta, profiles)
- **Performance Tuning**: 256 max connections, 128 threads, 64MB block size

#### S3A Scheme Support

The plugin includes a **custom S3ALoader** that extends Paimon's S3 support to handle `s3a://` scheme URLs:

- **Background**: Paimon's built-in S3Loader only supports `s3://` scheme
- **Solution**: Custom `S3ALoader` registers for `s3a://` scheme but uses the same underlying S3FileIO
- **Service Discovery**: Registered via `META-INF/services/org.apache.paimon.fs.FileIOLoader`
- **Credential Chain**: Uses DefaultAWSCredentialsProviderChain for maximum compatibility

## Field Processing Features

### Field Mapping

Transform field names during ingestion to match your nrtsearch index schema:

```yaml
ingestionPluginConfigs:
  paimon:
    field.mapping:
      source_user_id: "userId"          # Map 'source_user_id' to 'userId'
      product_title: "title"            # Map 'product_title' to 'title'
      internal_metadata: "metadata"     # Map 'internal_metadata' to 'metadata'
```

### Field Dropping

Drop unwanted fields by prefix to reduce index size and processing overhead:

```yaml
ingestionPluginConfigs:
  paimon:
    field.drop.prefixes:
      - "__internal__"                  # Drop all fields starting with '__internal__'
      - "__debug_"                      # Drop all fields starting with '__debug_'
      - "_temp_"                        # Drop all fields starting with '_temp_'
      - "sys_"                          # Drop all fields starting with 'sys_'
```

**Processing Order**: Fields are first checked for dropping, then remaining fields are mapped if configured, otherwise kept with original names.

## ID-Based Sharding

For distributed processing across multiple nrtsearch instances, the plugin supports ID-based sharding using modulo arithmetic.

### Configuration

```yaml
ingestionPluginConfigs:
  paimon:
    id_sharding_max: 30                 # Total number of shards (required)
    # Service name must end with shard number: "nrtsearch-service-23"
```

### Service Name Requirements

The plugin extracts the shard number from the service name:
- **Format**: Service name must end with `-{number}` (e.g., `nrtsearch-paimon-service-23`)
- **Shard Assignment**: Service processes records where `primary_key % id_sharding_max == service_number`
- **Example**: With `id_sharding_max: 30` and service `nrtsearch-service-23`, processes records where `photo_id % 30 == 23`

### How It Works

1. **Primary Key Detection**: Uses the first primary key from the Paimon table schema as the sharding field
2. **Modulo Filter**: Creates a predicate filter: `primary_key % id_sharding_max == service_number`
3. **Distributed Processing**: Each service instance processes only its assigned shard of data
4. **Fault Tolerance**: Service name parsing errors cause immediate startup failure for safety

### Example Setup

```yaml
# Service: nrtsearch-photos-7
ingestionPluginConfigs:
  paimon:
    database.name: "photos_db"
    table.name: "photos"                # Table with primary key 'photo_id'
    target.index.name: "photos-index"
    id_sharding_max: 10                 # Total of 10 services (0-9)
    # This service processes: photo_id % 10 == 7
```

## Dependency Packaging

### Critical Dependencies

The plugin requires careful dependency management to work in NRTSearch's plugin classloader environment:

#### Core Paimon Dependencies
```gradle
// Paimon core for standalone JVM (includes most FileIO implementations)
implementation 'org.apache.paimon:paimon-bundle:1.4-SNAPSHOT'

// Paimon's S3 implementation (contains S3FileIO and S3Loader)
implementation 'org.apache.paimon:paimon-s3:1.4-SNAPSHOT'

// Hadoop client for S3A filesystem and AWS credential providers
implementation 'org.apache.hadoop:hadoop-client:3.3.4'
implementation 'org.apache.hadoop:hadoop-aws:3.3.4'
```

#### Shadow JAR Configuration
```gradle
shadowJar {
    // Enable ZIP64 for large JARs (required for all dependencies)
    zip64 true
    archiveClassifier = 'all'

    // CRITICAL: Merge service files instead of overwriting
    mergeServiceFiles()
}
```

### Service File Merging

**Problem**: Multiple JARs contain `META-INF/services/org.apache.paimon.fs.FileIOLoader` files:
- `paimon-s3.jar`: Contains S3Loader registration
- Our plugin: Contains S3ALoader registration

**Solution**: `mergeServiceFiles()` combines all service registrations, enabling both loaders:
```
# Final merged service file
org.apache.paimon.s3.S3Loader
com.yelp.nrtsearch.plugins.ingestion.paimon.S3ALoader
```

### Classloader Considerations

#### Plugin Isolation
- NRTSearch loads plugins in isolated classloaders
- Each plugin's dependencies are self-contained via Shadow JAR
- Thread context classloader must be set for worker threads

#### Hadoop Configuration
- Hadoop `Configuration` objects must be created within plugin context
- S3A filesystem implementation is loaded dynamically
- Credential providers are resolved via classloader

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