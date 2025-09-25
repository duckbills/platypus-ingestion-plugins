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

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PaimonIngestPlugin provides Apache Paimon ingestion functionality for nrtsearch. Uses Dynamic
 * Shared Queue architecture to handle workload skew in dynamic bucket tables while maintaining
 * ordering guarantees similar to Kafka partitions.
 *
 * <p>See {@link PaimonConfig} for configuration options
 */
public class PaimonIngestPlugin extends Plugin implements IngestionPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonIngestPlugin.class);
  private static final AtomicInteger THREAD_ID = new AtomicInteger();

  private final NrtsearchConfig nrtsearchConfig;
  private final PaimonConfig paimonConfig;

  // Services
  private ExecutorService executorService;

  /**
   * Constructor for the PaimonIngestPlugin.
   *
   * @param config The NrtsearchConfig instance
   */
  public PaimonIngestPlugin(NrtsearchConfig config) {
    this.nrtsearchConfig = config;
    this.paimonConfig = getPaimonConfig(config);
  }

  private PaimonConfig getPaimonConfig(NrtsearchConfig config) {
    // Create PaimonConfig from NrtsearchConfig
    Map<String, Map<String, Object>> ingestionConfigs = config.getIngestionPluginConfigs();
    Map<String, Object> paimonConfigMap = ingestionConfigs.get("paimon");
    if (paimonConfigMap == null) {
      throw new IllegalStateException("Missing config for 'paimon' ingestion plugin");
    }
    return new PaimonConfig(paimonConfigMap);
  }

  /**
   * Get the Paimon configuration.
   *
   * @return the PaimonConfig instance
   */
  public PaimonConfig getPaimonConfig() {
    return paimonConfig;
  }

  @Override
  public Ingestor getIngestor() {
    return new PaimonIngestor(nrtsearchConfig, getIngestionExecutor(), paimonConfig);
  }

  @Override
  public ExecutorService getIngestionExecutor() {
    if (this.executorService == null) {
      int workerThreads = paimonConfig.getWorkerThreads();
      this.executorService =
          Executors.newFixedThreadPool(
              workerThreads + 1, // +1 for coordinator thread
              r -> {
                Thread t = new Thread(r, "paimon-ingest-worker-" + THREAD_ID.getAndIncrement());
                t.setDaemon(true);
                // Set thread context classloader to plugin's classloader for service discovery
                t.setContextClassLoader(this.getClass().getClassLoader());
                return t;
              });
    }
    return executorService;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Closing PaimonIngestPlugin and shutting down executor service");
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
    }
    super.close();
  }
}
