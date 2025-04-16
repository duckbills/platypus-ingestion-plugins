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
package com.yelp.nrtsearch.plugins.ingestion.common;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all ingestion plugins. This class provides common functionality for managing the
 * ingestion lifecycle and thread pool management.
 */
public abstract class AbstractIngestionPlugin extends Plugin {
  private static final Logger logger = LoggerFactory.getLogger(AbstractIngestionPlugin.class);

  protected final NrtsearchConfig config;
  protected final ExecutorService executorService;
  private volatile boolean isRunning;

  protected AbstractIngestionPlugin(NrtsearchConfig config, int threadPoolSize) {
    this.config = config;
    this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    this.isRunning = false;
  }

  /**
   * Start the ingestion process. This method should be called to begin data ingestion.
   * Implementations should override this method to initialize their specific ingestion logic.
   */
  public abstract void startIngestion();

  /**
   * Stop the ingestion process. This method should be called to gracefully shutdown ingestion.
   * Implementations should override this method to properly cleanup their resources.
   */
  public abstract void stopIngestion();

  /** Returns whether the ingestion process is currently running. */
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void close() throws IOException {
    logger.info("Shutting down ingestion plugin");
    if (isRunning) {
      stopIngestion();
    }
    executorService.shutdown();
  }

  protected void setRunning(boolean running) {
    this.isRunning = running;
  }
}
