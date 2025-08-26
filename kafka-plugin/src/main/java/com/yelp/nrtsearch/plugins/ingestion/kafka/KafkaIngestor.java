package com.yelp.nrtsearch.plugins.ingestion.kafka;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.ingestion.AbstractIngestor;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaIngestor extends AbstractIngestor {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIngestor.class);

  // kafka constants

  private static final int MAX_POLL_RECORDS = 1000; // Ensure we don't get too many records at once
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

  // Retry configuration - package-private for testing
  static final int DEFAULT_MAX_RETRIES = 3;
  static final long DEFAULT_RETRY_DELAY_MS = 5000; // 5 seconds

  // Allow overriding for tests
  private final int maxRetries;
  private final long retryDelayMs;

  private final IngestionConfig ingestionConfig;
  private final ExecutorService executorService;
  private final List<KafkaConsumer<String, Object>> consumers = new ArrayList<>();
  private final int numPartitions;
  private volatile boolean running;

  public KafkaIngestor(NrtsearchConfig config, ExecutorService executorService, int numPartitions) {
    this(config, executorService, numPartitions, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY_MS);
  }

  // Package-private constructor for testing with custom retry parameters
  KafkaIngestor(
      NrtsearchConfig config,
      ExecutorService executorService,
      int numPartitions,
      int maxRetries,
      long retryDelayMs) {
    super(config);
    this.ingestionConfig = getIngestionConfig(config);
    this.executorService = executorService;
    this.numPartitions = numPartitions;
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;
  }

  private IngestionConfig getIngestionConfig(NrtsearchConfig config) {
    // 1. Get all ingestion plugin configs (keyed by plugin name)
    Map<String, Map<String, Object>> ingestionPluginConfigs = config.getIngestionPluginConfigs();
    if (ingestionPluginConfigs == null) {
      throw new IllegalStateException("No ingestion plugins configured!");
    }
    // 2. Extract the kafka config
    Map<String, Object> kafkaConfig = ingestionPluginConfigs.get("kafka");
    if (kafkaConfig == null) {
      throw new IllegalStateException("No kafka plugin config under pluginConfigs.ingestion!");
    }
    // 3. Build your IngestionConfig object
    return new IngestionConfig(kafkaConfig);
  }

  @Override
  public void start() throws IOException {
    // start Kafka consumers one per partition
    running = true;
    for (int i = 0; i < numPartitions; i++) {
      KafkaConsumer<String, Object> consumer = createKafkaConsumer();
      consumers.add(consumer);
      int finalI = i;
      getExecutorService().submit(() -> consumePartition(consumer, finalI));
      LOGGER.info(
          "Started Kafka ingestion for topic: {}, indexing to: {}",
          ingestionConfig.getTopic(),
          ingestionConfig.getIndexName());
    }
  }

  @Override
  public void stop() {
    running = false;
    LOGGER.info("Initiating Kafka ingestion shutdown...");

    // Wake up all consumers (breaks them out of poll immediately)
    for (KafkaConsumer<String, Object> consumer : consumers) {
      try {
        consumer.wakeup();
      } catch (Exception e) {
        LOGGER.warn("Exception while waking up consumer: ", e);
      }
    }

    // Attempt to shutdown executor service gracefully
    getExecutorService().shutdown();
    boolean terminated = false;
    try {
      // Wait up to 5 seconds for graceful shutdown
      terminated = getExecutorService().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while awaiting executorService shutdown", ie);
    }

    if (!terminated) {
      LOGGER.warn("Forcing executorService shutdown now...");
      List<Runnable> dropped = getExecutorService().shutdownNow();
      if (!dropped.isEmpty()) {
        LOGGER.warn("Forced shutdown. {} tasks never commenced execution.", dropped.size());
      }
    }

    // Close all consumers, catch exceptions
    for (KafkaConsumer<String, Object> consumer : consumers) {
      try {
        consumer.close();
      } catch (Exception e) {
        LOGGER.warn("Exception while closing Kafka consumer: ", e);
      }
    }

    LOGGER.info("Kafka ingestion shutdown complete.");
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  private KafkaConsumer<String, Object> createKafkaConsumer() {
    // Auto commit should be disabled to ensure we only commit after successful indexing
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ingestionConfig.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, ingestionConfig.getConsumerGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ingestionConfig.getAutoOffsetReset());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Always false for safety
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
    props.put("schema.registry.url", ingestionConfig.getSchemaRegistryUrl());
    props.put("specific.avro.reader", false); // Use GenericRecord instead of specific classes
    return new KafkaConsumer<>(props);
  }

  private void consumePartition(KafkaConsumer<String, Object> consumer, int partition) {
    final TopicPartition topicPartition = new TopicPartition(ingestionConfig.getTopic(), partition);
    consumer.assign(Collections.singletonList(topicPartition));
    final AvroToAddDocumentConverter documentConverter = new AvroToAddDocumentConverter();
    final List<AddDocumentRequest> pendingDocuments = new ArrayList<>();
    final int batchSize = ingestionConfig.getBatchSize();

    try {
      while (running) {
        try {
          ConsumerRecords<String, Object> records = consumer.poll(POLL_TIMEOUT);
          if (records.isEmpty()) {
            // Commit if we have pending documents, even if no new messages
            if (!pendingDocuments.isEmpty()) {
              try {
                indexAndCommitBatch(pendingDocuments);
                consumer.commitSync();
                pendingDocuments.clear();
              } catch (Exception e) {
                LOGGER.error(
                    "Failed to index pending documents for partition {}, will retry in next poll",
                    partition,
                    e);
                // Don't clear pendingDocuments - keep them for retry
              }
            }
            continue;
          }
          // Process records for this partition
          for (ConsumerRecord<String, Object> record : records) {
            Object value = record.value();
            if (!(value instanceof GenericRecord)) {
              LOGGER.warn("Skipping non-Avro record in partition {}: {}", partition, value);
              continue;
            }
            AddDocumentRequest request =
                documentConverter.convertToNrtDocument(
                    (GenericRecord) value, ingestionConfig.getIndexName());
            pendingDocuments.add(request);

            // Commit batch if we've reached the batch size
            if (pendingDocuments.size() >= batchSize) {
              try {
                indexAndCommitBatch(pendingDocuments);
                consumer.commitSync();
                pendingDocuments.clear();
              } catch (Exception e) {
                LOGGER.error(
                    "Failed to index batch for partition {}, will retry in next poll",
                    partition,
                    e);
                // Don't clear pendingDocuments - keep them for retry
                break; // Exit inner loop to retry in next poll
              }
            }
          }
          // Commit any remaining documents (smaller than batch)
          if (!pendingDocuments.isEmpty()) {
            try {
              indexAndCommitBatch(pendingDocuments);
              consumer.commitSync();
              pendingDocuments.clear();
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to index remaining documents for partition {}, will retry in next poll",
                  partition,
                  e);
              // Don't clear pendingDocuments - keep them for retry
            }
          }
        } catch (Exception e) {
          LOGGER.error("Error processing messages for partition {}", partition, e);
          if (!running) break;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    } finally {
      try {
        consumer.close();
      } catch (Exception e) {
        LOGGER.warn("Exception closing consumer for partition {}", partition, e);
      }
    }
  }

  private void indexAndCommitBatch(List<AddDocumentRequest> docs) throws Exception {
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.addDocuments(docs, ingestionConfig.getIndexName());
        this.commit(ingestionConfig.getIndexName());
        return; // Success, exit retry loop

      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT
            && e.getMessage().contains("does not exist, unable to add documents")) {

          LOGGER.warn(
              "Index '{}' does not exist yet, attempt {}/{}, retrying in {}ms...",
              ingestionConfig.getIndexName(),
              attempt,
              maxRetries,
              retryDelayMs);

          if (attempt == maxRetries) {
            LOGGER.error(
                "Failed to index documents after {} attempts - index '{}' still doesn't exist",
                maxRetries,
                ingestionConfig.getIndexName());
            throw e; // Re-throw after max retries
          }

          try {
            Thread.sleep(retryDelayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry", ie);
          }
        } else {
          // Different error, don't retry
          throw e;
        }
      }
    }
  }
}
