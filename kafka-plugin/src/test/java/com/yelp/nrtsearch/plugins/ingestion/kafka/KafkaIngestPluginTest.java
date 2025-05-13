package com.yelp.nrtsearch.plugins.ingestion.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class KafkaIngestPluginTest {

  @Mock NrtsearchConfig mockConfig;

  private Map<String, Object> kafkaConfig;
  private Map<String, Map<String, Object>> pluginConfigs;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrapServers", "localhost:9092");
    kafkaConfig.put("groupId", "nrtsearch-kafka-consumer");
    kafkaConfig.put("topic", "test-topic");
    kafkaConfig.put("indexName", "test-index");
    kafkaConfig.put("autoOffsetReset", "earliest");

    pluginConfigs = new HashMap<>();
    pluginConfigs.put("kafka", kafkaConfig);

    when(mockConfig.getIngestionPluginConfigs()).thenReturn(pluginConfigs);
  }

  @Test
  public void testConstructorExtractsIngestionConfig() {
    KafkaIngestPlugin plugin = new KafkaIngestPlugin(mockConfig);
    assertNotNull(plugin);
    IngestionConfig ingestConfig = plugin.getIngestionConfig();
    assertEquals("test-topic", ingestConfig.getTopic());
    assertEquals("test-index", ingestConfig.getIndexName());
    assertEquals("localhost:9092", ingestConfig.getBootstrapServers());
    assertEquals("nrtsearch-kafka-consumer", ingestConfig.getConsumerGroupId());
    assertEquals("earliest", ingestConfig.getAutoOffsetReset());
  }

  @Test
  public void testMissingKafkaConfigThrows() {
    pluginConfigs.remove("kafka");
    assertThrows(IllegalStateException.class, () -> new KafkaIngestPlugin(mockConfig));
  }

  @Test
  public void testExecutorServiceIsSingleton() {
    KafkaIngestPlugin plugin = new KafkaIngestPlugin(mockConfig);
    ExecutorService exec1 = plugin.getIngestionExecutor();
    ExecutorService exec2 = plugin.getIngestionExecutor();
    assertNotNull(exec1);
    assertSame(exec1, exec2);
  }

  @Test
  public void testGetIngestorReturnsKafkaIngestor() {
    KafkaIngestPlugin plugin = new KafkaIngestPlugin(mockConfig);
    Ingestor ingestor = plugin.getIngestor();
    assertNotNull(ingestor);
    assertTrue(ingestor instanceof KafkaIngestor);
    KafkaIngestor kin = (KafkaIngestor) ingestor;
    assertSame(plugin.getIngestionExecutor(), kin.getExecutorService());
  }
}
