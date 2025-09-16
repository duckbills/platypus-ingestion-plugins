# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nrtsearch-ingestion-plugins** is a multi-module Gradle project that provides pluggable ingestion solutions for nrtSearch. This repository contains plugins that enable streaming data from various sources (Kafka, Apache Paimon data lakes, S3, etc.) into nrtSearch indexes using the ingestion plugin framework.

**Cross-Repository Context**: This project works in conjunction with:
- `/Users/umesh/code/open_source/nrtsearch` - Core nrtSearch server with S3 Express serverless architecture
- `/Users/umesh/code/open_source/paimon` - Apache Paimon data lake platform

## Architecture

### Multi-Module Structure
- **Root project**: Configuration and shared dependencies
- **kafka-plugin**: Kafka-based ingestion plugin (production-ready)
- **paimon-plugin**: Apache Paimon data lake ingestion plugin (in development)
- **Future plugins**: s3-plugin, file-plugin, etc.

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
./gradlew e2eTest               # End-to-end tests (*E2ETest.class)
./gradlew check                 # All tests including integration
```

### Plugin-Specific Commands
```bash
# Kafka Plugin
./gradlew :kafka-plugin:build
./gradlew :kafka-plugin:e2eTest
./gradlew :kafka-plugin:distTar  # Create plugin distribution

# Paimon Plugin
./gradlew :paimon-plugin:build
./gradlew :paimon-plugin:test
./gradlew :paimon-plugin:distTar  # Create plugin distribution
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

paimon-plugin/
├── src/main/java/              # Plugin implementation
│   └── com/yelp/nrtsearch/plugins/ingestion/paimon/
├── src/main/plugin-metadata/   # Plugin metadata
├── src/test/java/              # Unit tests (E2E tests planned)
│   └── com/yelp/nrtsearch/plugins/ingestion/paimon/
```

### Key Dependencies
- **nrtSearch**: Core server and plugin framework
- **Kafka Plugin**: `kafka-clients`, `kafka-schema-registry-client`, `kafka-avro-serializer`
- **Paimon Plugin**: `paimon-core`, `paimon-common`, `paimon-s3`, `paimon-hive-catalog`
- **Testing**: Testcontainers, JUnit 5, Mockito, Awaitility
- **Infrastructure**: gRPC, Protobuf, Avro

### Plugin Configuration
Plugins are configured via nrtSearch YAML:
```yaml
plugins:
  - kafka-plugin
  - paimon-plugin
pluginConfigs:
  ingestion:
    kafka:
      bootstrapServers: "localhost:9092"
      topic: "documents"
      groupId: "nrtsearch-ingestion"
    paimon:
      table.path: "warehouse.documents_table"
      target.index.name: "documents_index"
      warehouse.path: "s3://data-lake/warehouse"
      worker.threads: "4"
      batch.size: "1000"
```

## Plugin Specifics

### Kafka Plugin (Production Ready)

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

### Paimon Plugin (In Development)

#### Core Components
- **PaimonIngestPlugin**: Main plugin class implementing `IngestionPlugin`
- **PaimonIngestor**: Core ingestion logic with Dynamic Shared Queue architecture
- **PaimonToAddDocumentConverter**: Converts Paimon InternalRow to nrtSearch documents
- **PaimonConfig**: Configuration management and validation
- **InFlightBatch**: Coordination object for checkpoint management

#### Data Flow
1. **Plugin starts** → Paimon catalog connects to S3-backed data lake
2. **Coordinator loop** → Scans for incremental splits using stream table scan
3. **Worker threads** → Process bucket work with ordering guarantees
4. **Convert data** → Paimon InternalRow converted to `AddDocumentRequest`
5. **Index documents** → Framework handles indexing via `addDocuments()`
6. **Commit checkpoint** → Atomic checkpoint after all buckets complete

#### Dynamic Shared Queue Architecture
- **Coordinator Thread**: Discovers incremental work, groups by bucket, dispatches to queue
- **Worker Threads**: Process buckets from shared queue, maintain ordering within buckets
- **Incremental Checkpointing**: Uses Paimon's checkpoint/restore for exactly-once processing
- **Workload Skew Handling**: Dynamic bucket distribution prevents hot partition issues

#### Current Testing Status
- **Unit Tests**: ✅ PaimonConfig, PaimonIngestPlugin, InFlightBatch, PaimonToAddDocumentConverter
- **Unit Tests for PaimonIngestor**: 🚧 In Progress (next milestone)
- **E2E Tests**: 📋 Planned (requires real S3-backed Paimon table)

#### S3 Integration
Paimon plugin leverages S3 Express architecture from nrtSearch core:
- **S3-backed warehouse**: Tables stored in S3 with Paimon's lakehouse format
- **Stream processing**: Incremental reading of new data files
- **Regional co-location**: Optimal performance with S3 Express One Zone buckets

## Development Workflow

### Adding New Plugins
1. Create new subproject directory (e.g., `s3-plugin/`)
2. Add to `settings.gradle`: `include("s3-plugin")`
3. Create `build.gradle` with plugin-specific dependencies
4. Implement `IngestionPlugin` and extend `AbstractIngestor`
5. Add plugin metadata YAML
6. Write comprehensive tests (unit → integration → e2e)

### Paimon Plugin Development Roadmap
#### Current Status (January 2025)
- ✅ **Core Architecture**: Dynamic Shared Queue with coordinator-worker pattern
- ✅ **Configuration**: PaimonConfig with validation and field mapping
- ✅ **Plugin Integration**: PaimonIngestPlugin implementing framework interfaces
- ✅ **Data Conversion**: PaimonToAddDocumentConverter with schema handling
- ✅ **Unit Tests**: Config, plugin, converter, and batch coordination
- 🚧 **PaimonIngestor Unit Tests**: In progress - testing coordinator/worker logic
- 📋 **E2E Tests**: Planned - real S3 Paimon table → nrtSearch indexing → queries

#### Next Milestones
1. **Complete Unit Test Coverage**: PaimonIngestor coordinator/worker thread testing
2. **E2E Test Implementation**: Similar to KafkaIngestorE2ETest pattern
3. **Performance Benchmarking**: Compare with Kafka plugin throughput
4. **Production Deployment**: External company validation (Reddit/others)

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

#### Kafka Plugin E2E Tests
E2E tests require:
1. Testcontainers (Docker) for Kafka/Schema Registry
2. In-process nrtSearch server
3. Real data flow from Kafka → nrtSearch → Query

#### Paimon Plugin E2E Tests (Planned)
E2E tests will require:
1. S3-backed Paimon warehouse (via Testcontainers S3 mock or real AWS)
2. In-process nrtSearch server with paimon-plugin loaded
3. Real data flow: Paimon Table → PaimonIngestor → nrtSearch → Query
4. Incremental processing validation with checkpoints

### Plugin Distribution
```bash
# Kafka Plugin
./gradlew :kafka-plugin:distTar
# Creates: kafka-plugin/build/distributions/kafka-plugin-0.1.0-SNAPSHOT.tar

# Paimon Plugin
./gradlew :paimon-plugin:distTar
# Creates: paimon-plugin/build/distributions/paimon-plugin-0.1.0-SNAPSHOT.tar
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
- **Kafka**: Consumer API with Avro deserialization, Schema Registry integration
- **Paimon**: S3-backed data lake, streaming table scan, checkpoint/restore mechanism
- **S3**: Data lake storage, S3 Express integration for ultra-low latency
- **Testcontainers**: Reliable testing with real external services

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

## Current Development Focus

### Paimon Plugin Enhancement
**Immediate Goals**:
1. **Complete PaimonIngestor Unit Tests**: Test coordinator/worker logic, error handling, checkpointing
2. **Implement E2E Tests**: Follow KafkaIngestorE2ETest pattern for real S3 → nrtSearch validation
3. **Performance Validation**: Benchmark against Kafka plugin for throughput comparison

**E2E Test Requirements**:
- S3-backed Paimon warehouse with streaming table
- Real nrtSearch server with plugin loading
- Data ingestion → indexing → search query validation
- Incremental processing and checkpoint recovery testing

**Integration with nrtSearch Core**:
- Leverage S3 Express serverless architecture for optimal Paimon performance
- Regional co-location of compute and S3 Express storage
- Support for vector search workloads via Paimon's columnar format