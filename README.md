# NRTSearch Ingestion Plugins

This repository contains a collection of ingestion plugins for [NRTSearch](https://github.com/yelp/nrtsearch). These plugins enable real-time data ingestion from various sources into NRTSearch indexes with full end-to-end testing infrastructure.

## Available Plugins

### Kafka Ingestion Plugin
A production-ready plugin that consumes data from Apache Kafka topics and indexes it into NRTSearch. Features include:
- **Avro Schema Support**: Full integration with Confluent Schema Registry
- **Reliable Ingestion**: Configurable batch sizing, offset tracking, and crash recovery
- **Performance Optimized**: Concurrent consumer threads with backpressure handling
- **Production Ready**: Comprehensive error handling and monitoring support
- **Flexible Configuration**: Support for complex nested Avro schemas

## Quick Start

### Prerequisites
- **Java 21+** (required for NRTSearch compatibility)
- **Docker** (for E2E tests with Testcontainers)
- **Gradle 8.6+** (included via wrapper)
- **Git** for cloning repositories

### Setup for Development

1. **Clone the repositories**:
```bash
# Clone adjacent to each other (required for build dependencies)
git clone https://github.com/yelp/nrtsearch.git
git clone https://github.com/umeshdangat/nrtsearch-ingestion-plugins.git
```

2. **Build everything**:
```bash
cd nrtsearch-ingestion-plugins
./setup.sh
```

This automated setup will:
- Build and install NRTSearch to your local Maven repository
- Compile all ingestion plugins with dependencies
- Verify the build with unit tests

3. **Verify installation**:
```bash
# Run all tests (unit + E2E)
./gradlew test e2eTest

# Build plugin distributions
./gradlew distZip
```

### Testing Your Setup

The repository includes comprehensive testing at multiple levels:

**Unit Tests** (fast, no external dependencies):
```bash
./gradlew test
```

**End-to-End Tests** (full Kafka + Schema Registry via Docker):
```bash
./gradlew e2eTest
```

The E2E tests automatically:
- Start real Kafka and Schema Registry containers
- Test complete data flow: Avro producer → Kafka → Plugin → NRTSearch → Search queries
- Validate schema compatibility and field mapping
- Test plugin loading and configuration

## Plugin Configuration

### Kafka Plugin Configuration

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

#### Configuration Parameters
- **bootstrapServers**: Kafka broker addresses
- **schemaRegistryUrl**: Confluent Schema Registry URL for Avro schema management
- **groupId**: Kafka consumer group ID (use unique names for different deployments)
- **topic**: Kafka topic to consume from
- **indexName**: NRTSearch index to write documents to
- **autoOffsetReset**: What to do when there's no initial offset (`earliest` or `latest`)
- **batchSize**: Number of documents to batch before committing to NRTSearch
- **maxPollRecords**: Maximum records returned in a single poll()
- **pollTimeoutMs**: Timeout for Kafka consumer poll operations

### Schema Requirements

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

## Development Guide

### Contributing to Existing Plugins

1. **Fork and clone** the repository
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make changes** with proper tests
4. **Run the full test suite**: `./gradlew test e2eTest`
5. **Submit a pull request**

### Adding a New Ingestion Plugin

Follow these steps to create a new ingestion source (e.g., Pulsar, RabbitMQ, etc.):

#### 1. Create Plugin Structure
```bash
# Create the plugin module
mkdir -p new-plugin/src/main/java/com/yelp/nrtsearch/plugins/ingestion/newplugin
mkdir -p new-plugin/src/main/plugin-metadata
mkdir -p new-plugin/src/test/java/com/yelp/nrtsearch/plugins/ingestion/newplugin/{unit,integration,e2e}
```

#### 2. Configure the Module
Add to `settings.gradle`:
```groovy
include 'new-plugin'
```

Create `new-plugin/build.gradle`:
```groovy
dependencies {
    implementation libs.nrtsearch.server
    implementation libs.slf4j.api
    // Add your specific dependencies (e.g., Pulsar client, etc.)
    
    testImplementation(\"${libs.nrtsearch.server.get()}:tests\")
    testImplementation libs.junit
    testImplementation libs.testcontainers.core
    // Add testcontainer modules for your technology
}

// Copy test configuration from kafka-plugin/build.gradle
test { useJUnitPlatform() }
// Add e2eTest tasks
```

#### 3. Implement Core Classes

**Main Plugin Class**:
```java
// new-plugin/src/main/java/.../NewPlugin.java
public class NewPlugin extends AbstractIngestionPlugin {
    @Override
    public void start() {
        // Initialize your ingestion service
    }
    
    @Override 
    public void stop() {
        // Clean shutdown
    }
}
```

**Plugin Metadata**:
```yaml
# new-plugin/src/main/plugin-metadata/plugin-metadata.yaml  
name: nrtsearch-new-ingestion
version: ${version}
description: New ingestion plugin for NRTSearch
classname: ${classname}
nrtsearch_version: ${server_version}
```

#### 4. Add Comprehensive Tests

**Unit Tests** (`src/test/java/.../unit/`):
- Test individual component logic
- Mock external dependencies
- Fast execution (< 1s per test)

**Integration Tests** (`src/test/java/.../integration/`):
- Test component integration  
- Use Testcontainers for external services
- Medium execution time (5-30s per test)

**E2E Tests** (`src/test/java/.../e2e/`):
- Full end-to-end validation
- Real NRTSearch server + your service + Docker containers
- Complete data flow validation
- Follow the pattern in `KafkaIngestorE2ETest.java`

#### 5. Testing Best Practices

**For E2E Tests**:
```java
public class NewIngestorE2ETest extends NrtsearchTest {
    // Use Testcontainers for your service
    @BeforeClass
    public static void setup() {
        // Start your service containers
        // Publish test data
    }
    
    @Override
    protected List<String> getPlugins() {
        // Return path to your built plugin
    }
    
    @Test 
    public void testEndToEndIngestion() {
        // Setup NRTSearch index
        // Wait for ingestion with awaitility
        // Verify documents via search
    }
}
```

### Development Workflow

#### Running Tests During Development
```bash
# Fast feedback loop - unit tests only
./gradlew :your-plugin:test


# Full E2E validation (slower but comprehensive)  
./gradlew :your-plugin:e2eTest

# Everything
./gradlew check
```

#### Debugging E2E Tests
E2E tests use Testcontainers and can be debugged:
```bash
# Enable test container logging
TESTCONTAINERS_REUSE_ENABLE=true ./gradlew :kafka-plugin:e2eTest --info

# Keep containers running for manual inspection
./gradlew :kafka-plugin:e2eTest -Dtestcontainers.reuse.enable=true
```

#### Building Plugin Distributions
```bash
# Build ZIP distributions for deployment
./gradlew distZip

# Find built plugins in build/distributions/
ls */build/distributions/*.zip
```

## Architecture

### Plugin Loading Mechanism
NRTSearch plugins are loaded dynamically at server startup:
1. **Distribution**: Plugins are packaged as ZIP files containing JARs and metadata
2. **Storage**: Plugin ZIPs can be stored locally or in S3 (recommended for production)
3. **Loading**: NRTSearch downloads and extracts plugins to a local cache directory
4. **Initialization**: Plugin classes are loaded via custom ClassLoader and instantiated

### Data Flow (Kafka Plugin Example)
```
Kafka Topic → Avro Consumer → Schema Registry → Field Mapping → NRTSearch Index → Search API
     ↓              ↓              ↓              ↓              ↓
   Offset      Deserialization   Type         Document      Commit &
  Management      & Batching    Conversion    Indexing      Refresh
```

### Error Handling & Recovery
- **Offset Management**: Consumer offsets only committed after successful NRTSearch indexing
- **Retry Logic**: Configurable retry with exponential backoff for transient failures  
- **Dead Letter Queues**: Failed messages can be redirected to error topics
- **Graceful Shutdown**: Plugins complete in-flight operations before stopping

## Troubleshooting

### Common Issues

#### E2E Tests Failing with Docker Issues
```bash
# Check Docker daemon is running
docker info

# For Rancher Desktop users - ensure correct socket path
ls -la ~/.rd/docker.sock

# Enable Testcontainers debug logging
./gradlew e2eTest --info -Dtestcontainers.dockersocket.path=/Users/$USER/.rd/docker.sock
```

#### Plugin Loading Failures
```bash
# Verify plugin ZIP structure
unzip -l build/distributions/kafka-plugin-0.1.0-SNAPSHOT.zip

# Check NRTSearch server logs for ClassNotFoundException or plugin errors
tail -f nrtsearch-server.log | grep -i plugin
```

#### Schema Registry Connectivity
```bash
# Test Schema Registry connectivity
curl http://localhost:8081/subjects

# Check Avro schema registration
curl http://localhost:8081/subjects/your-topic-value/versions/latest
```

#### Performance Tuning
- **Batch Size**: Increase `batchSize` for higher throughput (default: 1000)
- **Consumer Threads**: Plugin supports multiple consumer threads per partition
- **JVM Settings**: Ensure adequate heap for large Avro schemas and batching

### Getting Help

1. **Check logs**: Enable DEBUG level logging for `com.yelp.nrtsearch.plugins`
2. **Run tests**: Use E2E tests to reproduce issues in controlled environment  
3. **Community**: Open issues in the GitHub repository with:
   - Full error logs
   - Configuration details  
   - Steps to reproduce

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Code style guidelines
- Pull request process  
- Issue reporting templates
- Development environment setup

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.