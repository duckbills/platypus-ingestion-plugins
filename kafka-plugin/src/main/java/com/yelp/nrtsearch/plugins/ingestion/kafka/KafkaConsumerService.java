package com.yelp.nrtsearch.plugins.ingestion.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerService {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

  private final KafkaConsumer<String, Object> consumer;
  private final SchemaRegistryClient schemaRegistryClient;

  public KafkaConsumerService(
      String brokers, String topic, String groupId, String schemaRegistryUrl) {
    // Configure Kafka consumer
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put("schema.registry.url", schemaRegistryUrl);

    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Collections.singletonList(topic));

    // Initialize Schema Registry client
    this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
  }

  public void consumeMessages() {
    while (true) {
      ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, Object> record : records) {
        logger.info(
            "Consumed message: key = {}, value = {}, offset = {}",
            record.key(),
            record.value(),
            record.offset());
        // Process the message (e.g., index into nrtsearch)
      }
      // Commit offsets after processing
      consumer.commitSync();
      logger.debug("Offsets committed successfully");
    }
  }
}
