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

import com.yelp.nrtsearch.plugins.ingestion.paimon.utils.ConfigHelper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating ShardingStrategy instances from configuration.
 *
 * <p>Supported strategies:
 *
 * <ul>
 *   <li><b>none</b>: No sharding - process all data
 *   <li><b>modulo</b>: Modulo-based sharding with partition-level file pruning
 *   <li><b>geo</b>: Geographic sharding (not yet implemented)
 * </ul>
 */
public class ShardingStrategyFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStrategyFactory.class);

  public static ShardingStrategy create(Map<String, Object> shardingConfig) {
    if (shardingConfig == null || shardingConfig.isEmpty()) {
      LOGGER.info("No sharding configuration - using NoShardingStrategy");
      return new NoShardingStrategy();
    }

    String strategy = ConfigHelper.getString(shardingConfig, "strategy");

    return switch (strategy.toLowerCase()) {
      case "none" -> {
        LOGGER.info("Creating NoShardingStrategy");
        yield new NoShardingStrategy();
      }
      case "modulo" -> createModuloStrategy(shardingConfig);
      case "geo" -> createGeoStrategy();
      default ->
          throw new IllegalArgumentException(
              String.format(
                  "Unknown sharding strategy: '%s'. Supported strategies: none, modulo, geo",
                  strategy));
    };
  }

  private static ShardingStrategy createModuloStrategy(Map<String, Object> shardingConfig) {
    int maxShards = ConfigHelper.getInteger(shardingConfig, "modulo.max_shards");
    String partitionField = ConfigHelper.getString(shardingConfig, "modulo.partition_field");

    LOGGER.info(
        "Creating ModuloShardingStrategy: max_shards={}, partition_field={}",
        maxShards,
        partitionField);

    return new ModuloShardingStrategy(maxShards, partitionField);
  }

  private static ShardingStrategy createGeoStrategy() {
    LOGGER.warn("Geo sharding requested but not yet implemented");
    return new GeoShardingStrategy();
  }
}
