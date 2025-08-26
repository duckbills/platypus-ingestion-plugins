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

import java.util.Map;

/**
 * Configuration for Kafka ingestion.
 *
 * <p>Expected YAML configuration structure:
 *
 * <pre>
 * pluginConfigs:
 *   ingestion:
 *     kafka:
 *       bootstrapServers: "localhost:9092"  # Kafka bootstrap servers (default: localhost:9092)
 *       groupId: "nrtsearch-kafka-consumer"  # Kafka consumer group ID (default: nrtsearch-kafka-consumer)
 *       topic: "my-topic"  # Kafka topic to consume (required)
 *       autoCommitEnabled: false  # Whether to enable auto-commit (default: false)
 *       autoOffsetReset: "earliest"  # Auto offset reset strategy (default: earliest)
 *       schemaRegistryUrl: "http://localhost:8081"  # Confluent Schema Registry URL (optional)
 *       indexName: "my-index"  # NrtSearch index name (required)
 *       autoRegisterFields: true  # Automatically register fields from Avro schema (default: false)
 * </pre>
 */
public class IngestionConfig {
  private static final String CONFIG_PREFIX = "pluginConfigs.ingestion.kafka";

  // Default values
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String DEFAULT_CONSUMER_GROUP_ID = "nrtsearch-kafka-consumer";
  private static final boolean DEFAULT_AUTO_COMMIT_ENABLED = false;
  private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
  private static final int DEFAULT_BATCH_SIZE = 1000;

  private final String bootstrapServers;
  private final String consumerGroupId;
  private final String topic;
  private final boolean autoCommitEnabled;
  private final String autoOffsetReset;
  private final String schemaRegistryUrl;
  private final String indexName;
  private final boolean autoRegisterFields;
  private final String batchSize;

  /**
   * Create a configuration object using values from the YAML configuration.
   *
   * @param Map: "pluginsConfig.ingestion.kafka" from NrtSearchConfig
   * @throws IllegalStateException if the plugins.ingestion section or kafka config is not present
   */
  @SuppressWarnings("unchecked")
  public IngestionConfig(Map<String, Object> configMap) {
    this.bootstrapServers =
        (String) configMap.getOrDefault("bootstrapServers", DEFAULT_BOOTSTRAP_SERVERS);
    this.consumerGroupId = (String) configMap.getOrDefault("groupId", DEFAULT_CONSUMER_GROUP_ID);
    this.topic = (String) configMap.get("topic");
    this.autoCommitEnabled =
        Boolean.parseBoolean(
            String.valueOf(
                configMap.getOrDefault("autoCommitEnabled", DEFAULT_AUTO_COMMIT_ENABLED)));
    this.autoOffsetReset =
        (String) configMap.getOrDefault("autoOffsetReset", DEFAULT_AUTO_OFFSET_RESET);
    this.schemaRegistryUrl = (String) configMap.get("schemaRegistryUrl");
    this.indexName = (String) configMap.get("indexName");
    this.autoRegisterFields =
        Boolean.parseBoolean(String.valueOf(configMap.getOrDefault("autoRegisterFields", false)));
    this.batchSize =
        String.valueOf(configMap.getOrDefault("batchSize", DEFAULT_BATCH_SIZE));
    validate();
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
      boolean autoRegisterFields,
      String batchSize) {
    this.bootstrapServers = bootstrapServers;
    this.consumerGroupId = consumerGroupId;
    this.topic = topic;
    this.autoCommitEnabled = autoCommitEnabled;
    this.autoOffsetReset = autoOffsetReset;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.indexName = indexName;
    this.autoRegisterFields = autoRegisterFields;
    this.batchSize = batchSize;

    // Validate config for constructor usage as well
    validate();
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

    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      throw new IllegalStateException("Bootstrap servers are required");
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

  public int getBatchSize() {
    return Integer.parseInt(batchSize);
  }

  public boolean isAutoRegisterFields() {
    return autoRegisterFields;
  }

  @Override
  public String toString() {
    return "IngestionConfig{"
        + "bootstrapServers='"
        + bootstrapServers
        + '\''
        + ", consumerGroupId='"
        + consumerGroupId
        + '\''
        + ", topic='"
        + topic
        + '\''
        + ", autoCommitEnabled="
        + autoCommitEnabled
        + ", autoOffsetReset='"
        + autoOffsetReset
        + '\''
        + ", schemaRegistryUrl='"
        + schemaRegistryUrl
        + '\''
        + ", indexName='"
        + indexName
        + '\''
        + ", autoRegisterFields="
        + autoRegisterFields
        + ", batchSize='"
        + batchSize
        + '\''
        + '}';
  }
}
