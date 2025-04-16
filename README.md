# NRTSearch Ingestion Plugins

This repository contains a collection of ingestion plugins for [NRTSearch](https://github.com/yelp/nrtsearch). These plugins enable data ingestion from various sources into NRTSearch indexes.

## Available Plugins

### Kafka Ingestion Plugin
A plugin that consumes data from Apache Kafka topics and indexes it into NRTSearch. Features include:
- Schema synchronization with Confluent Schema Registry
- Configurable batch sizing and commit strategies
- Offset tracking for reliable ingestion
- Crash recovery support

## Building

### Prerequisites
- Java 21 or later
- Gradle
- Local clone of nrtsearch repository (adjacent to this repository)

### Build Instructions

1. Clone the repositories:
```bash
git clone https://github.com/yelp/nrtsearch.git
git clone https://github.com/yelp/nrtsearch-ingestion-plugins.git
```

2. Build the project:
```bash
cd nrtsearch-ingestion-plugins
./setup.sh
```

This will:
1. Build and install nrtsearch to your local maven repository
2. Build all ingestion plugins

## Plugin Configuration

### Kafka Plugin
The Kafka plugin requires the following configuration:
```yaml
plugins:
  - nrtsearch-kafka-ingestion

plugin_configs:
  nrtsearch-kafka-ingestion:
    bootstrap_servers: "localhost:9092"
    schema_registry_url: "http://localhost:8081"
    group_id: "nrtsearch-consumer"
    topic: "your-topic-name"
    batch_size: 1000
```

## Development

### Adding a New Plugin
1. Create a new module in the project:
```bash
mkdir -p new-plugin/src/main/java/com/yelp/nrtsearch/plugins/ingestion/newplugin
```

2. Add the module to `settings.gradle`:
```groovy
include 'new-plugin'
```

3. Create a new plugin class that extends `AbstractIngestionPlugin`

4. Add plugin metadata in `src/main/plugin-metadata/plugin-metadata.yaml`

### Testing
Run tests with:
```bash
./gradlew test
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.