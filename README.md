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

### Paimon Ingestion Plugin
A production-ready plugin for ingesting data from Apache Paimon data lake tables with advanced S3A support. Features include:
- **S3A Filesystem Support**: Custom S3ALoader extends Paimon's S3 support to `s3a://` schemes
- **Dynamic Shared Queue**: Coordinator-worker pattern handles workload skew in dynamic bucket tables
- **Incremental Processing**: Efficient checkpoint/restore mechanism for real-time data processing
- **Complex Type Conversion**: Robust handling of arrays, embeddings, and nested data structures
- **Production S3 Integration**: IAM roles, credential providers, and performance optimization
- **Self-Contained Dependencies**: Shadow JAR with merged service files for plugin isolation

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

Each plugin has its own configuration format and requirements. See the individual plugin READMEs for detailed configuration instructions:

- **Kafka Plugin**: [kafka-plugin/README.md](kafka-plugin/README.md#configuration)
- **Paimon Plugin**: [paimon-plugin/README.md](paimon-plugin/README.md#configuration)

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
TESTCONTAINERS_REUSE_ENABLE=true ./gradlew :nrtsearch-plugin-kafka:e2eTest --info

# Keep containers running for manual inspection
./gradlew :nrtsearch-plugin-kafka:e2eTest -Dtestcontainers.reuse.enable=true
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

### Data Flow
Each plugin implements a different data flow pattern optimized for its source system:
- **Kafka Plugin**: Event-driven consumption with offset management
- **Paimon Plugin**: Batch processing with dynamic work distribution

See individual plugin READMEs for detailed architecture diagrams.

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
unzip -l build/distributions/nrtsearch-plugin-kafka-0.1.0-SNAPSHOT.zip

# Check NRTSearch server logs for ClassNotFoundException or plugin errors
tail -f nrtsearch-server.log | grep -i plugin
```

#### Plugin-Specific Issues

For plugin-specific troubleshooting:
- **Kafka Plugin**: [kafka-plugin/README.md](kafka-plugin/README.md#troubleshooting)
- **Paimon Plugin**: [paimon-plugin/README.md](paimon-plugin/README.md#s3a-filesystem-troubleshooting)

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