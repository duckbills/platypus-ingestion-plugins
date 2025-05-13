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
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaIngestPlugin provides Kafka ingestion functionality for nrtsearch. It consumes messages from
 * a Kafka topic and processes them for indexing. TODO: support automatic schema conversion from
 * Avro schemas in Confluent Schema Registry.
 *
 * <p>See {@link IngestionConfig} for configuration options
 */
public class KafkaIngestPlugin implements IngestionPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIngestPlugin.class);
  private static final int DEFAULT_NUM_PARTITIONS = 1;
  private static final AtomicInteger THREAD_ID = new AtomicInteger();
  private final int numPartitions =
      DEFAULT_NUM_PARTITIONS; // TODO: replace with number of kafka partitions

  private final NrtsearchConfig nrtsearchConfig;
  private final IngestionConfig ingestionConfig;

  private static final int DEFAULT_BATCH_SIZE = 1000;

  // Services
  private AvroToAddDocumentConverter documentConverter;
  private IndexState indexState;
  private ExecutorService executorService;

  /**
   * Constructor for the KafkaIngestPlugin.
   *
   * @param config The NrtsearchConfig instance
   */
  public KafkaIngestPlugin(NrtsearchConfig config) {
    this.nrtsearchConfig = config;
    this.ingestionConfig = getIngestionConfig(config);
  }

  private IngestionConfig getIngestionConfig(NrtsearchConfig config) {
    // Create IngestionConfig from NrtsearchConfig
    Map<String, Map<String, Object>> ingestionConfigs = config.getIngestionPluginConfigs();
    Map<String, Object> kafkaConfig = ingestionConfigs.get("kafka");
    if (kafkaConfig == null) {
      throw new IllegalStateException("Missing config for 'kafka' ingestion plugin");
    }
    return new IngestionConfig(kafkaConfig);
  }

  /**
   * Get the ingestion configuration.
   *
   * @return the IngestionConfig instance
   */
  public IngestionConfig getIngestionConfig() {
    return ingestionConfig;
  }

  @Override
  public Ingestor getIngestor() {
    return new KafkaIngestor(nrtsearchConfig, getIngestionExecutor(), numPartitions);
  }

  @Override
  public ExecutorService getIngestionExecutor() {
    if (this.executorService == null) {
      this.executorService =
          Executors.newFixedThreadPool(
              numPartitions,
              r -> {
                Thread t =
                    new Thread(r, "kafka-ingest-partition-consumer-" + THREAD_ID.getAndIncrement());
                t.setDaemon(true);
                return t;
              });
    }
    return executorService;
  }
}
