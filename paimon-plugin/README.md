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

    # Sharding configuration (optional) - for distributed processing
    sharding:
      strategy: "modulo"                           # Sharding strategy: none, modulo, geo
      modulo:
        max_shards: 30                             # Total number of shards
        partition_field: "__internal_partition_id" # Partition field name in Paimon table
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

## Sharding for Distributed Processing

The plugin supports multiple sharding strategies to enable horizontal scaling across multiple nrtsearch service instances. Sharding divides data processing workload for improved throughput and fault tolerance.

### Strategy: None (Default)

Process all data without sharding.

**Use when:**
- Single instance deployment
- Small datasets
- Development/testing

**Configuration:**
```yaml
# Option 1: Explicit none strategy
sharding:
  strategy: "none"

# Option 2: Omit sharding config entirely (default behavior)
```

### Strategy: Modulo (Partition-Level File Pruning) ⭐ Recommended

Distributed processing using partition-level file skipping for maximum I/O efficiency.

**Benefits:**
- **~N× I/O reduction** (N = number of shards) - each instance reads only 1/N of files from S3
- **File-level skipping** - Paimon skips entire segments at manifest level before reading data
- **No row-level filtering overhead** - data pre-filtered at partition boundary
- **Optimal S3 costs** - dramatically reduces S3 API calls and data transfer

**Requirements:**
1. **Partitioned Paimon table**: Table MUST use `PARTITIONED BY (partition_field)`
2. **Upstream calculation**: Partition field MUST contain `primary_key % max_shards`
3. **Service name format**: Must end with shard number (0 to max_shards-1)
4. **Drop partition field**: Use `field.drop.prefixes` to exclude partition field from nrtsearch index (it's only needed for file filtering)

**Configuration:**
```yaml
serviceName: "nrtsearch-photos-23"  # Service name must end with shard number

ingestionPluginConfigs:
  paimon:
    database.name: "production"
    table.name: "photos"
    target.index.name: "photos-index"
    warehouse.path: "s3a://prod-data-lake/warehouse/"

    # Drop internal partition field - not needed in nrtsearch index
    field.drop.prefixes:
      - "__internal_"                              # Drops __internal_partition_id

    sharding:
      strategy: "modulo"
      modulo:
        max_shards: 30                             # Total number of shards
        partition_field: "__internal_partition_id" # Partition field name in Paimon table
```

**Upstream Flink Configuration:**

Your Flink job must create a partitioned table with the partition field calculated correctly:

```sql
-- Create partitioned Paimon table
CREATE TABLE photos_partitioned (
    photo_id BIGINT NOT NULL,
    title STRING,
    description STRING,
    __internal_partition_id INT NOT NULL,       -- Partition field
    PRIMARY KEY (photo_id) NOT ENFORCED
) PARTITIONED BY (__internal_partition_id);     -- Partition by modulo result

-- Insert data with partition calculation
INSERT INTO photos_partitioned
SELECT
    photo_id,
    title,
    description,
    photo_id % 30 AS __internal_partition_id    -- Calculate partition using modulo
FROM source_photos;
```

**Service Name Requirements:**

Service name must end with a number in range `[0, max_shards)`:
- ✅ `nrtsearch-photos-0` → shard 0
- ✅ `prod-service-15` → shard 15
- ✅ `my_service_29` → shard 29
- ❌ `nrtsearch-photos` → ERROR: no shard number
- ❌ `service-30` → ERROR: out of range for max_shards=30

**How It Works:**

Partition-level file pruning works by filtering at the **metadata level** before reading any data files from S3.

**Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│ Paimon Manifest File (metadata - SMALL, always read)       │
├─────────────────────────────────────────────────────────────┤
│ Segment 1: file=data-001.parquet, partition=0, bucket=0    │
│ Segment 2: file=data-002.parquet, partition=1, bucket=1    │
│ Segment 3: file=data-003.parquet, partition=2, bucket=1 ✅ │
│ Segment 4: file=data-004.parquet, partition=2, bucket=0 ✅ │
│ Segment 5: file=data-005.parquet, partition=3, bucket=0    │
│ ...                                                         │
│ Segment 30: file=data-030.parquet, partition=29, bucket=2  │
└─────────────────────────────────────────────────────────────┘
                        ↓
            partitionFilter.test(partition == 2)
                        ↓
        ┌───────────────┴──────────────────┐
        ↓                                  ↓
    TRUE (✅)                          FALSE (❌)
    Read these files:                  SKIP - Don't read from S3:
    - data-003.parquet                 - data-001.parquet
    - data-004.parquet                 - data-002.parquet
                                       - data-005.parquet
                                       - ... (28 files skipped)
```

**Step-by-Step Process:**

1. **Service Name → Shard ID**: Extract shard number from service name (e.g., `nrtsearch-photos-2` → shard 2)
2. **Create Partition Filter**: `Map.of("__internal_partition_id", "2")` → filter predicate
3. **Read Manifest Metadata**: Paimon reads small manifest file containing file→partition mappings
4. **Filter at Metadata Level**: For each file entry, test `partition == 2` against metadata
5. **Skip Non-Matching Files**: Files with partition ≠ 2 are **never read from S3**
6. **Read Only Matching Files**: Only files with partition = 2 are fetched and processed

**Why This is Efficient:**

| Approach | Reads Manifest | Reads Data Files | S3 API Calls | Data Transfer |
|----------|----------------|------------------|--------------|---------------|
| **No Sharding** | Yes (1 read) | All files (30) | High | 100% |
| **Row-Level Filtering** | Yes (1 read) | All files (30) | High | 100% then filter |
| **Partition-Level Filtering** | Yes (1 read) | Matching files (1) | Low (~3%) | ~3.3% |

**Key Insight**: Partition information is **pre-computed metadata** stored in the manifest. The filter decision happens in memory before any data file I/O, enabling true file-level skipping.

**Validation:**

The plugin validates configuration at startup:
- ✅ Verifies table is partitioned
- ✅ Checks partition field exists in table schema
- ✅ Validates service name ends with valid shard number
- ❌ Fails fast with clear error messages if misconfigured

Example error:
```
IllegalArgumentException: Service name 'nrtsearch-photos' must end with shard number (0-29).
Examples: 'nrtsearch-service-0', 'my_service_5'
```

### Strategy: Geo (Future)

Geographic sharding based on region or data center. Not yet implemented.

```yaml
sharding:
  strategy: "geo"
  # Configuration TBD
```

### Sharding Strategy Comparison

| Strategy | I/O Reduction | Table Requirements | Service Name Format | Use Case |
|----------|---------------|-------------------|---------------------|----------|
| **none** | None | Any table | Any name | Single instance, dev/test |
| **modulo** | ~N× (N=shards) | `PARTITIONED BY (field)` | Must end with number (0 to N-1) | Production, distributed processing |
| **geo** | Varies | TBD | TBD | Multi-region deployments |

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