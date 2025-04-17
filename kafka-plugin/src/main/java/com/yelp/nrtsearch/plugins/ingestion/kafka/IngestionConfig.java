/*
 * Copyright 2024 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.plugins.ingestion.kafka;

import com.yelp.nrtsearch.server.config.YamlConfigReader;

/**
 * Configuration for Kafka ingestion.
 *
 * <p>Expected YAML configuration structure:
 *
 * <pre>
 * ingestion:
 *   kafka:
 *     bootstrapServers: "localhost:9092"  # Kafka bootstrap servers (default: localhost:9092)
 *     groupId: "nrtsearch-kafka-consumer"  # Kafka consumer group ID (default: nrtsearch-kafka-consumer)
 *     topic: "my-topic"  # Kafka topic to consume (required)
 *     autoCommitEnabled: false  # Whether to enable auto-commit (default: false)
 *     autoOffsetReset: "earliest"  # Auto offset reset strategy (default: earliest)
 *     schemaRegistryUrl: "http://localhost:8081"  # Confluent Schema Registry URL (optional)
 *     indexName: "my-index"  # NrtSearch index name (required)
 *     autoRegisterFields: true  # Automatically register fields from Avro schema (default: false)
 * </pre>
 */
public class IngestionConfig {
  // Default values
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String DEFAULT_CONSUMER_GROUP_ID = "nrtsearch-kafka-consumer";
  private static final boolean DEFAULT_AUTO_COMMIT_ENABLED = false;
  private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

  private final String bootstrapServers;
  private final String consumerGroupId;
  private final String topic;
  private final boolean autoCommitEnabled;
  private final String autoOffsetReset;
  private final String schemaRegistryUrl;
  private final String indexName;
  private final boolean autoRegisterFields;

  /**
   * Create a configuration object using values from the YAML configuration.
   *
   * @param configReader The YamlConfigReader to read configuration values from
   */
  public IngestionConfig(YamlConfigReader configReader) {
    this.bootstrapServers =
        configReader.getString("ingestion.kafka.bootstrapServers", DEFAULT_BOOTSTRAP_SERVERS);

    this.consumerGroupId =
        configReader.getString("ingestion.kafka.groupId", DEFAULT_CONSUMER_GROUP_ID);

    this.topic = configReader.getString("ingestion.kafka.topic", null);

    this.autoCommitEnabled =
        configReader.getBoolean("ingestion.kafka.autoCommitEnabled", DEFAULT_AUTO_COMMIT_ENABLED);

    this.autoOffsetReset =
        configReader.getString("ingestion.kafka.autoOffsetReset", DEFAULT_AUTO_OFFSET_RESET);

    this.schemaRegistryUrl = configReader.getString("ingestion.kafka.schemaRegistryUrl", null);

    this.indexName = configReader.getString("ingestion.kafka.indexName", null);

    this.autoRegisterFields = configReader.getBoolean("ingestion.kafka.autoRegisterFields", false);
  }

  /**
   * Constructor for IngestionConfig with explicit values (mainly for testing).
   *
   * @param bootstrapServers Kafka bootstrap servers
   * @param consumerGroupId Kafka consumer group ID
   * @param topic Kafka topic name
   * @param autoCommitEnabled Whether auto commit is enabled
   * @param autoOffsetReset Auto offset reset strategy
   * @param schemaRegistryUrl Confluent Schema Registry URL
   * @param indexName NrtSearch index name
   * @param autoRegisterFields Whether to automatically register fields
   */
  public IngestionConfig(
      String bootstrapServers,
      String consumerGroupId,
      String topic,
      boolean autoCommitEnabled,
      String autoOffsetReset,
      String schemaRegistryUrl,
      String indexName,
      boolean autoRegisterFields) {
    this.bootstrapServers = bootstrapServers;
    this.consumerGroupId = consumerGroupId;
    this.topic = topic;
    this.autoCommitEnabled = autoCommitEnabled;
    this.autoOffsetReset = autoOffsetReset;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.indexName = indexName;
    this.autoRegisterFields = autoRegisterFields;
  }

  /**
   * Validate that the configuration has all required values.
   *
   * @throws IllegalStateException if required values are missing
   */
  public void validate() {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalStateException("Kafka topic is required");
    }

    if (indexName == null || indexName.isEmpty()) {
      throw new IllegalStateException("Index name is required");
    }
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public String getTopic() {
    return topic;
  }

  public boolean isAutoCommitEnabled() {
    return autoCommitEnabled;
  }

  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public String getIndexName() {
    return indexName;
  }

  public boolean isAutoRegisterFields() {
    return autoRegisterFields;
  }
}
