/*
 * Copyright 2025 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0
 */
package com.yelp.nrtsearch.plugins.ingestion.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaIngestorTest {
  private KafkaIngestor ingestor;
  private ExecutorService executorService;
  private NrtsearchConfig mockConfig;

  @BeforeEach
  public void setUp() {
    executorService = Executors.newSingleThreadExecutor();

    Map<String, Object> kafkaSettings = new HashMap<>();
    kafkaSettings.put("bootstrapServers", "localhost:9092");
    kafkaSettings.put("groupId", "test-group");
    kafkaSettings.put("topic", "test-topic");
    kafkaSettings.put("autoCommitEnabled", false);
    kafkaSettings.put("autoOffsetReset", "earliest");
    kafkaSettings.put("schemaRegistryUrl", "http://localhost:8081");
    kafkaSettings.put("indexName", "test-index");
    kafkaSettings.put("autoRegisterFields", false);
    kafkaSettings.put("batchSize", "10");

    Map<String, Map<String, Object>> ingestionPluginConfigs = new HashMap<>();
    ingestionPluginConfigs.put("kafka", kafkaSettings);

    mockConfig = mock(NrtsearchConfig.class);
    when(mockConfig.getIngestionPluginConfigs()).thenReturn(ingestionPluginConfigs);

    ingestor = new KafkaIngestor(mockConfig, executorService, 1);
  }

  @Test
  public void testConstructorInitializesCorrectly() {
    assertNotNull(ingestor.getExecutorService());
  }

  @Test
  public void testStartAndStopDoesNotThrow() throws IOException {
    ingestor.stop(); // Safe to call stop without start
  }

  @Test
  public void testInvalidKafkaConfigThrows() {
    NrtsearchConfig badConfig = mock(NrtsearchConfig.class);
    when(badConfig.getIngestionPluginConfigs()).thenReturn(Collections.emptyMap());
    Exception ex =
        assertThrows(
            IllegalStateException.class,
            () -> {
              new KafkaIngestor(badConfig, executorService, 1);
            });
    assertTrue(ex.getMessage().contains("No kafka plugin config"));
  }
}
