package com.yelp.nrtsearch.plugins.ingestion.kafka.unit;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.plugins.ingestion.kafka.IngestionConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class IngestionConfigTest {

  @Test
  public void testValidDefaults() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrapServers", "localhost:9092");
    config.put("groupId", "nrtsearch-kafka-consumer");
    config.put("autoCommitEnabled", false);
    config.put("autoOffsetReset", "earliest");
    config.put("autoRegisterFields", false);
    config.put("topic", "test-topic"); // required
    config.put("indexName", "test-index"); // required
    config.put("schemaRegistryUrl", null);
    config.put("batchSize", "1000");

    IngestionConfig ingestionConfig = new IngestionConfig(config);

    assertEquals("localhost:9092", ingestionConfig.getBootstrapServers());
    assertEquals("nrtsearch-kafka-consumer", ingestionConfig.getConsumerGroupId());
    assertEquals("test-topic", ingestionConfig.getTopic());
    assertEquals("test-index", ingestionConfig.getIndexName());
    assertEquals("earliest", ingestionConfig.getAutoOffsetReset());
    assertFalse(ingestionConfig.isAutoCommitEnabled());
    assertFalse(ingestionConfig.isAutoRegisterFields());
    assertNull(ingestionConfig.getSchemaRegistryUrl());
    assertEquals(1000, ingestionConfig.getBatchSize());
  }

  @Test
  public void testCustomValues() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrapServers", "kafka1:9092,kafka2:9092");
    config.put("groupId", "custom-consumer-group");
    config.put("autoCommitEnabled", true);
    config.put("autoOffsetReset", "latest");
    config.put("autoRegisterFields", true);
    config.put("topic", "custom-topic");
    config.put("indexName", "custom-index");
    config.put("schemaRegistryUrl", "http://registry:8081");
    config.put("batchSize", "500");

    IngestionConfig ingestionConfig = new IngestionConfig(config);

    assertEquals("kafka1:9092,kafka2:9092", ingestionConfig.getBootstrapServers());
    assertEquals("custom-consumer-group", ingestionConfig.getConsumerGroupId());
    assertEquals("custom-topic", ingestionConfig.getTopic());
    assertEquals("custom-index", ingestionConfig.getIndexName());
    assertEquals("latest", ingestionConfig.getAutoOffsetReset());
    assertTrue(ingestionConfig.isAutoCommitEnabled());
    assertTrue(ingestionConfig.isAutoRegisterFields());
    assertEquals("http://registry:8081", ingestionConfig.getSchemaRegistryUrl());
    assertEquals(500, ingestionConfig.getBatchSize());
  }

  @Test
  public void testExplicitConstructor() {
    IngestionConfig config =
        new IngestionConfig(
            "broker:9092",
            "group-A",
            "topic-A",
            true,
            "latest",
            "http://schema-registry",
            "index-A",
            true,
            "300");

    assertEquals("broker:9092", config.getBootstrapServers());
    assertEquals("group-A", config.getConsumerGroupId());
    assertEquals("topic-A", config.getTopic());
    assertTrue(config.isAutoCommitEnabled());
    assertEquals("latest", config.getAutoOffsetReset());
    assertEquals("http://schema-registry", config.getSchemaRegistryUrl());
    assertEquals("index-A", config.getIndexName());
    assertTrue(config.isAutoRegisterFields());
    assertEquals(300, config.getBatchSize());
  }

  @Test
  public void testMissingTopicThrows() {
    Map<String, Object> config = new HashMap<>();
    config.put("indexName", "test-index");
    try {
      new IngestionConfig(config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ex) {
      assertEquals("Kafka topic is required", ex.getMessage());
    }
  }

  @Test
  public void testEmptyTopicThrows() {
    Map<String, Object> config = new HashMap<>();
    config.put("topic", "");
    config.put("indexName", "test-index");
    try {
      new IngestionConfig(config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ex) {
      assertEquals("Kafka topic is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingIndexNameThrows() {
    Map<String, Object> config = new HashMap<>();
    config.put("topic", "test-topic");

    try {
      new IngestionConfig(config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ex) {
      assertEquals("Index name is required", ex.getMessage());
    }
  }

  @Test
  public void testEmptyIndexNameThrows() {
    Map<String, Object> config = new HashMap<>();
    config.put("topic", "test-topic");
    config.put("indexName", "");

    try {
      new IngestionConfig(config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ex) {
      assertEquals("Index name is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingBootstrapServersThrows() {
    Map<String, Object> config = new HashMap<>();
    config.put("topic", "test-topic");
    config.put("indexName", "test-index");
    config.put("bootstrapServers", ""); // invalid empty
    try {
      new IngestionConfig(config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ex) {
      assertEquals("Bootstrap servers are required", ex.getMessage());
    }
  }
}
