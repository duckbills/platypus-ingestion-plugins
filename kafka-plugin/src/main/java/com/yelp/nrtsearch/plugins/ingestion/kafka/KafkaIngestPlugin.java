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

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaIngestPlugin provides Kafka ingestion functionality for nrtsearch. It consumes messages from
 * a Kafka topic and processes them for indexing. It also supports automatic schema conversion from
 * Avro schemas in Confluent Schema Registry.
 *
 * <p>See {@link IngestionConfig} for configuration options
 */
public class KafkaIngestPlugin extends Plugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIngestPlugin.class);
  private static final int DEFAULT_THREAD_POOL_SIZE = 1;

  private final NrtsearchConfig nrtsearchConfig;
  private final IngestionConfig ingestionConfig;
  private final ExecutorService executorService;
  private volatile boolean running = false;
  private KafkaConsumer<String, String> consumer;

  // Services
  private SchemaConversionService schemaConversionService;

  /**
   * Constructor for the KafkaIngestPlugin.
   *
   * @param config The NrtsearchConfig instance
   */
  public KafkaIngestPlugin(NrtsearchConfig config) {
    this.nrtsearchConfig = config;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);

    // Create IngestionConfig using YamlConfigReader from NrtsearchConfig
    this.ingestionConfig = new IngestionConfig(config.getConfigReader());

    // Initialize schema conversion service if schema registry URL is provided
    if (ingestionConfig.getSchemaRegistryUrl() != null
        && !ingestionConfig.getSchemaRegistryUrl().isEmpty()) {
      this.schemaConversionService =
          new SchemaConversionService(ingestionConfig.getSchemaRegistryUrl());
    }
  }

  /**
   * Get the ingestion configuration.
   *
   * @return the IngestionConfig instance
   */
  public IngestionConfig getIngestionConfig() {
    return ingestionConfig;
  }

  /** Start the Kafka ingestion process. */
  public void startIngestion() {
    if (isRunning()) {
      LOGGER.warn("Kafka ingestion is already running");
      return;
    }

    // Validate required configuration
    ingestionConfig.validate();

    // Auto-register fields if enabled and schema registry is configured
    if (ingestionConfig.isAutoRegisterFields() && schemaConversionService != null) {
      try {
        registerFieldsFromAvroSchema();
      } catch (Exception e) {
        LOGGER.error("Failed to auto-register fields from Avro schema", e);
        // Continue with ingestion even if field registration fails
      }
    }

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ingestionConfig.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, ingestionConfig.getConsumerGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ingestionConfig.getAutoOffsetReset());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ingestionConfig.isAutoCommitEnabled());

    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(ingestionConfig.getTopic()));
    setRunning(true);

    // Start consumer in a separate thread
    executorService.submit(this::consumeMessages);
    LOGGER.info(
        "Started Kafka ingestion for topic: {}, indexing to: {}",
        ingestionConfig.getTopic(),
        ingestionConfig.getIndexName());
  }

  /** Consume messages from Kafka and process them. */
  private void consumeMessages() {
    // Implementation of consumer loop here
    // TODO: Implement actual consumer loop and document indexing
  }

  /**
   * Register fields from Avro schema in the Schema Registry. We'll use the API but delegate the
   * actual registration to a controller/manager class that has access to the IndexState.
   */
  private void registerFieldsFromAvroSchema() throws IOException {
    if (schemaConversionService == null) {
      LOGGER.warn("Cannot register fields: Schema conversion service is not configured");
      return;
    }

    LOGGER.info(
        "Auto-registering fields from Avro schema for topic: {}", ingestionConfig.getTopic());

    // Convert Avro schema to FieldDefRequest
    FieldDefRequest fieldDefRequest =
        schemaConversionService.generateFieldDefRequest(
            ingestionConfig.getIndexName(), ingestionConfig.getTopic(), false);

    // We can't directly access the IndexState from the plugin, so we'll log the field definitions
    // and rely on an external mechanism to register them
    LOGGER.info(
        "Generated field definitions for index {}: {}",
        ingestionConfig.getIndexName(),
        fieldDefRequest.getFieldList());

    // In a real implementation, this would be done by a controller/manager that has access to the
    // IndexState

    LOGGER.info("Successfully processed fields from Avro schema");
  }

  /** Stop the Kafka ingestion process. */
  public void stopIngestion() {
    if (!isRunning()) {
      LOGGER.warn("Kafka ingestion is not running");
      return;
    }

    setRunning(false);
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }

    // Wait for executor to finish
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    LOGGER.info("Stopped Kafka ingestion");
  }

  /**
   * Check if the ingestion process is currently running.
   *
   * @return true if ingestion is running, false otherwise
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Set the running state of the ingestion process.
   *
   * @param running the new running state
   */
  protected void setRunning(boolean running) {
    this.running = running;
  }

  @Override
  public void close() throws IOException {
    stopIngestion();
  }
}
