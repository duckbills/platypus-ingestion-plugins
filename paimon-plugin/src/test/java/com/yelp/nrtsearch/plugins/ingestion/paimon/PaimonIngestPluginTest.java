/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.plugins.ingestion.paimon;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.junit.Test;

public class PaimonIngestPluginTest {

  @Test
  public void testPluginInitialization() {
    NrtsearchConfig config = createMockConfig();
    PaimonIngestPlugin plugin = new PaimonIngestPlugin(config);

    assertNotNull(plugin.getPaimonConfig());
    assertNotNull(plugin.getIngestor());
    assertNotNull(plugin.getIngestionExecutor());
  }

  @Test
  public void testGetIngestor() {
    NrtsearchConfig config = createMockConfig();
    PaimonIngestPlugin plugin = new PaimonIngestPlugin(config);

    Ingestor ingestor = plugin.getIngestor();
    assertTrue(ingestor instanceof PaimonIngestor);
  }

  @Test
  public void testGetIngestionExecutor() {
    NrtsearchConfig config = createMockConfig();
    PaimonIngestPlugin plugin = new PaimonIngestPlugin(config);

    ExecutorService executor = plugin.getIngestionExecutor();
    assertNotNull(executor);
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingPaimonConfig() {
    NrtsearchConfig config = mock(NrtsearchConfig.class);

    Map<String, Map<String, Object>> emptyConfigs = new HashMap<>();
    when(config.getIngestionPluginConfigs()).thenReturn(emptyConfigs);

    new PaimonIngestPlugin(config);
  }

  private NrtsearchConfig createMockConfig() {
    NrtsearchConfig config = mock(NrtsearchConfig.class);

    Map<String, Object> paimonConfig = new HashMap<>();
    paimonConfig.put("database.name", "test_db");
    paimonConfig.put("table.name", "test_table");
    paimonConfig.put("target.index.name", "test_index");
    paimonConfig.put("warehouse.path", "/tmp/paimon");
    paimonConfig.put("worker.threads", "4");

    Map<String, Map<String, Object>> pluginConfigs = new HashMap<>();
    pluginConfigs.put("paimon", paimonConfig);

    when(config.getIngestionPluginConfigs()).thenReturn(pluginConfigs);

    return config;
  }
}
