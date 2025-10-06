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

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

/**
 * Strategy for sharding Paimon data across multiple nrtSearch instances.
 *
 * <p>Implementations determine how to filter/partition data for a specific shard. Different
 * strategies may use different mechanisms:
 *
 * <ul>
 *   <li><b>Partition-level filtering</b>: Skip reading entire partitions/files from S3
 *   <li><b>Row-level filtering</b>: Read all files but filter rows (less efficient)
 *   <li><b>No filtering</b>: Process all data (single instance deployment)
 * </ul>
 *
 * <p>Example strategies:
 *
 * <ul>
 *   <li>{@link NoShardingStrategy}: Process all data without filtering
 *   <li>{@link ModuloShardingStrategy}: Partition-level pruning using modulo arithmetic
 *   <li>{@link GeoShardingStrategy}: Partition-level pruning by geographic region (future)
 * </ul>
 */
public interface ShardingStrategy {

  /**
   * Apply this sharding strategy to the ReadBuilder.
   *
   * <p>This method configures the Paimon ReadBuilder to read only the data relevant to this shard.
   * It may add partition filters, row filters, or no filters depending on the strategy.
   *
   * @param readBuilder The Paimon ReadBuilder to configure
   * @param table The Paimon table being read
   * @param serviceName The nrtSearch service name (used for shard ID calculation)
   * @return The configured ReadBuilder
   * @throws IllegalStateException if strategy cannot be applied (e.g., table not partitioned)
   */
  ReadBuilder applySharding(ReadBuilder readBuilder, Table table, String serviceName);

  /**
   * Validate this strategy can be applied to the given table.
   *
   * <p>Called during initialization to fail fast if the strategy is incompatible with the table
   * schema. For example, ModuloShardingStrategy requires a partitioned table.
   *
   * @param table The Paimon table
   * @throws IllegalArgumentException if strategy is incompatible with table
   */
  void validateTable(Table table);

  /**
   * Get human-readable description of this strategy for logging.
   *
   * <p>Should include key configuration details like shard count, partition field, etc.
   *
   * @return Description string
   */
  String getDescription();
}
