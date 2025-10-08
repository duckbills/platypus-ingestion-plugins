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

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for Apache Paimon ingestion plugin. Handles configuration for Dynamic Shared
 * Queue architecture and Paimon table access.
 */
public class PaimonConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonConfig.class);

  // Default configuration values
  private static final int DEFAULT_WORKER_THREADS = 4;
  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_POLL_TIMEOUT_MS = 1000L;
  private static final long DEFAULT_SCAN_INTERVAL_MS = 30000L; // 30 seconds
  private static final int DEFAULT_QUEUE_CAPACITY = 10000;
  private static final long DEFAULT_CHECKPOINT_TIMEOUT_MINUTES = 10L; // 10 minutes

  // Paimon table configuration
  private final String databaseName;
  private final String tableName;
  private final String tablePath;
  private final String targetIndexName;
  private final String warehousePath;

  // Dynamic Shared Queue configuration
  private final int workerThreads;
  private final int batchSize;
  private final long pollTimeoutMs;
  private final long scanIntervalMs;
  private final int queueCapacity;
  private final long checkpointTimeoutMinutes;

  // Field mapping configuration
  private final Map<String, String> fieldMapping;

  // Field drop configuration
  private final List<String> fieldDropPrefixes;

  // Sharding configuration
  private final Map<String, Object> shardingConfig;

  public PaimonConfig(Map<String, Object> config) {
    // Required configuration
    this.databaseName = getRequiredString(config, "database.name");
    this.tableName = getRequiredString(config, "table.name");
    this.tablePath = this.databaseName + "." + this.tableName;
    this.targetIndexName = getRequiredString(config, "target.index.name");
    this.warehousePath = getRequiredString(config, "warehouse.path");

    // Optional configuration with defaults
    this.workerThreads = getOptionalInt(config, "worker.threads", DEFAULT_WORKER_THREADS);
    this.batchSize = getOptionalInt(config, "batch.size", DEFAULT_BATCH_SIZE);
    this.pollTimeoutMs = getOptionalLong(config, "poll.timeout.ms", DEFAULT_POLL_TIMEOUT_MS);
    this.scanIntervalMs = getOptionalLong(config, "scan.interval.ms", DEFAULT_SCAN_INTERVAL_MS);
    this.queueCapacity = getOptionalInt(config, "queue.capacity", DEFAULT_QUEUE_CAPACITY);
    this.checkpointTimeoutMinutes =
        getOptionalLong(config, "checkpoint.timeout.minutes", DEFAULT_CHECKPOINT_TIMEOUT_MINUTES);

    // Field mapping (optional)
    @SuppressWarnings("unchecked")
    Map<String, String> fieldMappingRaw = (Map<String, String>) config.get("field.mapping");
    this.fieldMapping = fieldMappingRaw;

    // Field drop prefixes (optional)
    @SuppressWarnings("unchecked")
    List<String> fieldDropPrefixesRaw = (List<String>) config.get("field.drop.prefixes");
    this.fieldDropPrefixes = fieldDropPrefixesRaw;

    // Sharding configuration (optional)
    @SuppressWarnings("unchecked")
    Map<String, Object> shardingConfigRaw = (Map<String, Object>) config.get("sharding");
    this.shardingConfig = shardingConfigRaw;

    LOGGER.info(
        "Initialized PaimonConfig: database={}, table={}, tablePath={}, targetIndex={}, workerThreads={}, batchSize={}",
        databaseName,
        tableName,
        tablePath,
        targetIndexName,
        workerThreads,
        batchSize);
  }

  private String getRequiredString(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Required configuration '" + key + "' is missing");
    }
    return value.toString();
  }

  private int getOptionalInt(Map<String, Object> config, String key, int defaultValue) {
    Object value = config.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Integer) {
      return (Integer) value;
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid integer value for {}: {}, using default: {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  private long getOptionalLong(Map<String, Object> config, String key, long defaultValue) {
    Object value = config.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Long) {
      return (Long) value;
    }
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid long value for {}: {}, using default: {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  private Integer getOptionalInteger(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (value == null) {
      return null;
    }
    if (value instanceof Integer) {
      return (Integer) value;
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid integer value for {}: {}, ignoring", key, value);
      return null;
    }
  }

  // Getters
  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTablePath() {
    return tablePath;
  }

  public String getTargetIndexName() {
    return targetIndexName;
  }

  public String getWarehousePath() {
    return warehousePath;
  }

  public int getWorkerThreads() {
    return workerThreads;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public long getPollTimeoutMs() {
    return pollTimeoutMs;
  }

  public long getScanIntervalMs() {
    return scanIntervalMs;
  }

  public int getQueueCapacity() {
    return queueCapacity;
  }

  public long getCheckpointTimeoutMinutes() {
    return checkpointTimeoutMinutes;
  }

  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }

  public List<String> getFieldDropPrefixes() {
    return fieldDropPrefixes;
  }

  public Map<String, Object> getShardingConfig() {
    return shardingConfig;
  }
}
