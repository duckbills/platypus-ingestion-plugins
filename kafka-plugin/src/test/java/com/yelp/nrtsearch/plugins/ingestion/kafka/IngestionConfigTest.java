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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.YamlConfigReader;
import org.junit.Test;

public class IngestionConfigTest {

  @Test
  public void testDefaultValues() {
    // Mock the YamlConfigReader
    YamlConfigReader mockReader = mock(YamlConfigReader.class);

    // Set up the mock to return default values for all non-required configs
    when(mockReader.getString("ingestion.kafka.bootstrapServers", "localhost:9092"))
        .thenReturn("localhost:9092");
    when(mockReader.getString("ingestion.kafka.groupId", "nrtsearch-kafka-consumer"))
        .thenReturn("nrtsearch-kafka-consumer");
    when(mockReader.getBoolean("ingestion.kafka.autoCommitEnabled", false)).thenReturn(false);
    when(mockReader.getString("ingestion.kafka.autoOffsetReset", "earliest"))
        .thenReturn("earliest");
    when(mockReader.getBoolean("ingestion.kafka.autoRegisterFields", false)).thenReturn(false);

    // Set up required config values
    when(mockReader.getString("ingestion.kafka.topic", null)).thenReturn("test-topic");
    when(mockReader.getString("ingestion.kafka.indexName", null)).thenReturn("test-index");
    when(mockReader.getString("ingestion.kafka.schemaRegistryUrl", null)).thenReturn(null);

    // Create config from reader
    IngestionConfig config = new IngestionConfig(mockReader);

    // Verify default values
    assertEquals("localhost:9092", config.getBootstrapServers());
    assertEquals("nrtsearch-kafka-consumer", config.getConsumerGroupId());
    assertEquals("test-topic", config.getTopic());
    assertFalse(config.isAutoCommitEnabled());
    assertEquals("earliest", config.getAutoOffsetReset());
    assertEquals(null, config.getSchemaRegistryUrl());
    assertEquals("test-index", config.getIndexName());
    assertFalse(config.isAutoRegisterFields());
  }

  @Test
  public void testCustomValues() {
    // Mock the YamlConfigReader
    YamlConfigReader mockReader = mock(YamlConfigReader.class);

    // Set up the mock to return custom values for all configs
    when(mockReader.getString("ingestion.kafka.bootstrapServers", "localhost:9092"))
        .thenReturn("kafka1:9092,kafka2:9092");
    when(mockReader.getString("ingestion.kafka.groupId", "nrtsearch-kafka-consumer"))
        .thenReturn("custom-consumer-group");
    when(mockReader.getString("ingestion.kafka.topic", null)).thenReturn("custom-topic");
    when(mockReader.getBoolean("ingestion.kafka.autoCommitEnabled", false)).thenReturn(true);
    when(mockReader.getString("ingestion.kafka.autoOffsetReset", "earliest")).thenReturn("latest");
    when(mockReader.getString("ingestion.kafka.schemaRegistryUrl", null))
        .thenReturn("http://schema-registry:8081");
    when(mockReader.getString("ingestion.kafka.indexName", null)).thenReturn("custom-index");
    when(mockReader.getBoolean("ingestion.kafka.autoRegisterFields", false)).thenReturn(true);

    // Create config from reader
    IngestionConfig config = new IngestionConfig(mockReader);

    // Verify custom values
    assertEquals("kafka1:9092,kafka2:9092", config.getBootstrapServers());
    assertEquals("custom-consumer-group", config.getConsumerGroupId());
    assertEquals("custom-topic", config.getTopic());
    assertTrue(config.isAutoCommitEnabled());
    assertEquals("latest", config.getAutoOffsetReset());
    assertEquals("http://schema-registry:8081", config.getSchemaRegistryUrl());
    assertEquals("custom-index", config.getIndexName());
    assertTrue(config.isAutoRegisterFields());
  }

  @Test
  public void testExplicitConstructor() {
    // Create config with explicit values
    IngestionConfig config =
        new IngestionConfig(
            "server1:9092,server2:9092",
            "explicit-group",
            "explicit-topic",
            true,
            "latest",
            "http://registry:8081",
            "explicit-index",
            true);

    // Verify values
    assertEquals("server1:9092,server2:9092", config.getBootstrapServers());
    assertEquals("explicit-group", config.getConsumerGroupId());
    assertEquals("explicit-topic", config.getTopic());
    assertTrue(config.isAutoCommitEnabled());
    assertEquals("latest", config.getAutoOffsetReset());
    assertEquals("http://registry:8081", config.getSchemaRegistryUrl());
    assertEquals("explicit-index", config.getIndexName());
    assertTrue(config.isAutoRegisterFields());
  }

  @Test(expected = IllegalStateException.class)
  public void testValidate_noTopic() {
    // Create config with missing topic
    IngestionConfig config =
        new IngestionConfig(
            "localhost:9092",
            "test-group",
            null, // missing topic
            false,
            "earliest",
            null,
            "test-index",
            false);

    // Should throw IllegalStateException
    config.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testValidate_emptyTopic() {
    // Create config with empty topic
    IngestionConfig config =
        new IngestionConfig(
            "localhost:9092",
            "test-group",
            "", // empty topic
            false,
            "earliest",
            null,
            "test-index",
            false);

    // Should throw IllegalStateException
    config.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testValidate_noIndexName() {
    // Create config with missing index name
    IngestionConfig config =
        new IngestionConfig(
            "localhost:9092",
            "test-group",
            "test-topic",
            false,
            "earliest",
            null,
            null, // missing index name
            false);

    // Should throw IllegalStateException
    config.validate();
  }

  @Test(expected = IllegalStateException.class)
  public void testValidate_emptyIndexName() {
    // Create config with empty index name
    IngestionConfig config =
        new IngestionConfig(
            "localhost:9092",
            "test-group",
            "test-topic",
            false,
            "earliest",
            null,
            "", // empty index name
            false);

    // Should throw IllegalStateException
    config.validate();
  }
}
