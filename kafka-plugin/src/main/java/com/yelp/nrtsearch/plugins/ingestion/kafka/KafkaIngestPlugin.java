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

import com.yelp.nrtsearch.plugins.ingestion.common.AbstractIngestionPlugin;
import com.yelp.nrtsearch.plugins.ingestion.common.IngestionConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaIngestPlugin extends AbstractIngestionPlugin {
  private static final Logger logger = LoggerFactory.getLogger(KafkaIngestPlugin.class);
  private static final int DEFAULT_THREAD_POOL_SIZE = 1;

  private final IngestionConfig ingestionConfig;
  private KafkaConsumer<String, String> consumer;

  public KafkaIngestPlugin(NrtsearchConfig config) {
    super(config, DEFAULT_THREAD_POOL_SIZE);
    // Will be initialized when startIngestion is called
    this.ingestionConfig = null;
  }

  @Override
  public void startIngestion() {
    if (isRunning()) {
      logger.warn("Kafka ingestion is already running");
      return;
    }

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // TODO: from config
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "nrtsearch-kafka-consumer"); // TODO: from config
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    consumer = new KafkaConsumer<>(props);
    setRunning(true);

    // TODO: Implement consumer loop in a separate thread using executorService
    logger.info("Started Kafka ingestion plugin");
  }

  @Override
  public void stopIngestion() {
    if (!isRunning()) {
      logger.warn("Kafka ingestion is not running");
      return;
    }

    setRunning(false);
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }
    logger.info("Stopped Kafka ingestion plugin");
  }
}
