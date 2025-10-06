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
package com.yelp.nrtsearch.plugins.ingestion.paimon.sharding;

import java.util.List;
import java.util.Map;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modulo sharding strategy using partition-level file pruning.
 *
 * <p>This strategy requires:
 *
 * <ul>
 *   <li>Table MUST be partitioned by a field (e.g., <code>PARTITIONED BY (partition_field)
 *       </code>)
 *   <li>Partition field MUST contain: <code>primary_key % max_shards</code>
 *   <li>Upstream Flink/writer job MUST calculate partition field correctly
 * </ul>
 *
 * <p>Benefits:
 *
 * <ul>
 *   <li><b>Skips reading files entirely</b> from S3 for other partitions
 *   <li><b>~30x I/O reduction</b> for 30 shards (reads only 1/30 of files)
 *   <li><b>No row-level filtering overhead</b> - data already pre-filtered at file level
 * </ul>
 *
 * <p>Example upstream Flink SQL:
 *
 * <pre>{@code
 * CREATE TABLE partitioned_table (
 *     photo_id INT NOT NULL,
 *     __internal_partition_id INT NOT NULL,
 *     PRIMARY KEY (photo_id) NOT ENFORCED
 * ) PARTITIONED BY (__internal_partition_id);
 *
 * INSERT INTO partitioned_table
 * SELECT
 *     photo_id,
 *     photo_id % 30 AS __internal_partition_id,  -- Modulo calculation
 *     ...
 * FROM source_table;
 * }</pre>
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * sharding:
 *   strategy: "modulo"
 *   modulo:
 *     max_shards: 30
 *     partition_field: "__internal_partition_id"
 * }</pre>
 */
public class ModuloShardingStrategy implements ShardingStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModuloShardingStrategy.class);

  private final int maxShards;
  private final String partitionFieldName;

  /**
   * Create a modulo sharding strategy.
   *
   * @param maxShards Number of shards (must be positive)
   * @param partitionFieldName Name of partition field in table (required)
   * @throws IllegalArgumentException if maxShards <= 0 or partitionFieldName is null/empty
   */
  public ModuloShardingStrategy(int maxShards, String partitionFieldName) {
    if (maxShards <= 0) {
      throw new IllegalArgumentException("maxShards must be positive, got: " + maxShards);
    }
    if (partitionFieldName == null || partitionFieldName.trim().isEmpty()) {
      throw new IllegalArgumentException("partitionFieldName is required for modulo sharding");
    }
    this.maxShards = maxShards;
    this.partitionFieldName = partitionFieldName.trim();
  }

  @Override
  public void validateTable(Table table) {
    List<String> partitionKeys = table.partitionKeys();

    // Check table is partitioned
    if (partitionKeys.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Modulo sharding requires a PARTITIONED table. "
                  + "Table '%s' has no partition keys.%n"
                  + "Either:%n"
                  + "  1. Use a partitioned table with PARTITIONED BY (%s), OR%n"
                  + "  2. Set sharding.strategy = 'none' to process all data",
              table.name(), partitionFieldName));
    }

    // Check partition field exists
    if (!partitionKeys.contains(partitionFieldName)) {
      throw new IllegalArgumentException(
          String.format(
              "Partition field '%s' not found in table partition keys: %s. "
                  + "Ensure table is PARTITIONED BY (%s)",
              partitionFieldName, partitionKeys, partitionFieldName));
    }

    LOGGER.info(
        "✓ Validated modulo sharding: table '{}' is partitioned by '{}'",
        table.name(),
        partitionFieldName);
  }

  @Override
  public ReadBuilder applySharding(ReadBuilder readBuilder, Table table, String serviceName) {
    // Calculate which shard this instance should process
    int shardId = calculateShardId(serviceName, maxShards);

    LOGGER.info("=== MODULO SHARDING (PARTITION-LEVEL FILE PRUNING) ===");
    LOGGER.info("Shard ID: {}/{}", shardId, maxShards);
    LOGGER.info("Partition field: {}", partitionFieldName);
    LOGGER.info("Service name: {}", serviceName);

    Map<String, String> partitionSpec = Map.of(partitionFieldName, String.valueOf(shardId));
    ReadBuilder configured = readBuilder.withPartitionFilter(partitionSpec);

    LOGGER.info("✓ Applied partition filter: {} = {}", partitionFieldName, shardId);
    LOGGER.info("✓ Will SKIP reading files from {} other partitions", maxShards - 1);
    LOGGER.info("✓ Expected I/O reduction: ~{}x", maxShards);

    return configured;
  }

  @Override
  public String getDescription() {
    return String.format(
        "Modulo sharding: partition_field=%s, max_shards=%d (partition-level file pruning)",
        partitionFieldName, maxShards);
  }

  /**
   * Extract shard ID from service name suffix.
   *
   * <p>Service name must end with a number in range [0, maxShards). This ensures deterministic
   * shard assignment and matches the upstream partition calculation pattern.
   *
   * <p>Valid formats:
   *
   * <ul>
   *   <li>"nrtsearch-photos-23" → shard 23
   *   <li>"prod-service-0" → shard 0
   *   <li>"my_service_15" → shard 15
   * </ul>
   *
   * @param serviceName The nrtSearch service name (must end with number)
   * @param maxShards The maximum number of shards
   * @return Shard ID in range [0, maxShards)
   * @throws IllegalArgumentException if service name doesn't end with valid shard number
   */
  private static int calculateShardId(String serviceName, int maxShards) {
    // Extract trailing number from service name
    String[] parts = serviceName.split("[_-]");
    if (parts.length == 0) {
      throw new IllegalArgumentException(
          String.format(
              "Service name '%s' must end with shard number (0-%d). "
                  + "Examples: 'nrtsearch-service-0', 'my_service_5'",
              serviceName, maxShards - 1));
    }

    String lastPart = parts[parts.length - 1];
    int shardId;
    try {
      shardId = Integer.parseInt(lastPart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Service name '%s' must end with shard number (0-%d). "
                  + "Last segment '%s' is not a valid number. "
                  + "Examples: 'nrtsearch-service-0', 'my_service_5'",
              serviceName, maxShards - 1, lastPart),
          e);
    }

    // Validate shard ID is in valid range
    if (shardId < 0 || shardId >= maxShards) {
      throw new IllegalArgumentException(
          String.format(
              "Service name '%s' has shard ID %d, but must be in range [0, %d). "
                  + "Check that max_shards configuration matches your service naming convention.",
              serviceName, shardId, maxShards));
    }

    return shardId;
  }

  // Getters for testing
  public int getMaxShards() {
    return maxShards;
  }

  public String getPartitionFieldName() {
    return partitionFieldName;
  }
}
