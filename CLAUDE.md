# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nrtsearch-ingestion-plugins** is a multi-module Gradle project that provides pluggable ingestion solutions for nrtSearch. This repository contains plugins that enable streaming data from various sources (Kafka, S3, files, etc.) into nrtSearch indexes using the ingestion plugin framework.

## Architecture

### Multi-Module Structure
- **Root project**: Configuration and shared dependencies
- **kafka-plugin**: Kafka-based ingestion plugin
- **Future plugins**: paimon-plugin, s3-plugin, etc.

### Plugin Framework Integration
Plugins implement the nrtSearch ingestion framework interfaces:
- `IngestionPlugin`: Plugin contract providing `Ingestor` and `ExecutorService`
- `Ingestor`: Core ingestion logic interface with `start()`, `stop()`, `addDocuments()`, `commit()`
- `AbstractIngestor`: Base class providing common utilities

## Build Commands

### Basic Build and Test
```bash
./gradlew clean build
./gradlew test                    # Unit tests only
```

### Test Types
```bash
./gradlew integrationTest        # Integration tests (*IT.class)
./gradlew e2eTest               # End-to-end tests (*E2E.class)
./gradlew check                 # All tests including integration
```

### Plugin-Specific Commands
```bash
./gradlew :kafka-plugin:build
./gradlew :kafka-plugin:e2eTest
./gradlew :kafka-plugin:distTar  # Create plugin distribution
```

### Code Quality
```bash
./gradlew spotlessCheck         # Check code formatting
./gradlew spotlessApply         # Apply code formatting
```

## Plugin Development

### Directory Structure
```
kafka-plugin/
├── src/main/java/              # Plugin implementation
│   └── com/yelp/nrtsearch/plugins/ingestion/kafka/
├── src/main/plugin-metadata/   # Plugin metadata
├── src/test/java/
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests (*IT.class)
│   └── e2e/                    # E2E tests (*E2E.class)
```

### Key Dependencies
- **nrtSearch**: Core server and plugin framework
- **Kafka**: `kafka-clients`, `kafka-schema-registry-client`, `kafka-avro-serializer`
- **Testing**: Testcontainers, JUnit 5, Mockito, Awaitility
- **Infrastructure**: gRPC, Protobuf, Avro

### Plugin Configuration
Plugins are configured via nrtSearch YAML:
```yaml
plugins:
  - kafka-plugin
pluginConfigs:
  ingestion:
    kafka:
      bootstrapServers: "localhost:9092"
      topic: "documents"
      groupId: "nrtsearch-ingestion"
```

## Kafka Plugin Specifics

### Core Components
- **KafkaIngestPlugin**: Main plugin class implementing `IngestionPlugin`
- **KafkaIngestor**: Core ingestion logic extending `AbstractIngestor` 
- **AvroToAddDocumentConverter**: Converts Avro records to nrtSearch documents
- **IngestionConfig**: Configuration management and validation

### Data Flow
1. **Plugin starts** → Kafka consumer connects to specified topic
2. **Consume messages** → Avro records consumed from Kafka
3. **Convert data** → Avro records converted to `AddDocumentRequest`
4. **Index documents** → Framework handles indexing via `addDocuments()`
5. **Commit changes** → Periodic commits to ensure durability

### Testing Strategy
- **Unit Tests**: Mock-based testing of individual components
- **Integration Tests**: Testcontainers with real Kafka/Schema Registry
- **E2E Tests**: Full nrtSearch server + Kafka + indexing + querying

## Development Workflow

### Adding New Plugins
1. Create new subproject directory (e.g., `s3-plugin/`)
2. Add to `settings.gradle`: `include("s3-plugin")`
3. Create `build.gradle` with plugin-specific dependencies
4. Implement `IngestionPlugin` and extend `AbstractIngestor`
5. Add plugin metadata YAML
6. Write comprehensive tests

### Testing Guidelines
- **Unit tests**: Fast, isolated, mock dependencies
- **Integration tests**: Real external services (Kafka, etc.) via Testcontainers
- **E2E tests**: Full system tests with nrtSearch server

### Version Management
- Plugins version: `0.1.0-SNAPSHOT`
- nrtSearch dependency: `1.+` (latest compatible)
- Use version catalogs in `gradle/libs.versions.toml`

## Key Configuration Files

### Root Level
- `settings.gradle`: Multi-module configuration
- `build.gradle`: Shared configuration for all subprojects
- `gradle/libs.versions.toml`: Version catalog for dependencies

### Plugin Level
- `kafka-plugin/build.gradle`: Plugin-specific dependencies and test configuration
- `src/main/plugin-metadata/plugin-metadata.yaml`: Plugin metadata for nrtSearch

## Common Tasks

### Running E2E Tests
E2E tests require:
1. Testcontainers (Docker) for Kafka/Schema Registry
2. In-process nrtSearch server
3. Real data flow from Kafka → nrtSearch → Query

### Plugin Distribution
```bash
./gradlew :kafka-plugin:distTar
# Creates: kafka-plugin/build/distributions/kafka-plugin-0.1.0-SNAPSHOT.tar
```

### Debugging
- Use `testLogging.showStandardStreams = true` for detailed test output
- Testcontainers logs are available for debugging external services
- gRPC in-process server allows debugging nrtSearch interactions

## Integration Points

### With nrtSearch
- Plugins extend `AbstractIngestor` for framework integration
- Use `addDocuments()` and `commit()` for indexing operations
- Configuration loaded via `NrtsearchConfig.getIngestionPluginConfigs()`

### With External Systems
- Kafka: Consumer API with Avro deserialization
- Schema Registry: Automatic schema resolution and evolution
- Testcontainers: Reliable testing with real external services

## Development Notes

### Java Version
- Requires Java 21
- Uses Gradle toolchain for consistent builds

### Code Style
- Google Java Format via Spotless
- Automatic import optimization and formatting

### Testing Philosophy
- Comprehensive test coverage across unit/integration/e2e
- Real external services in tests (not mocks for integration)
- Fast feedback loop with separate test task types