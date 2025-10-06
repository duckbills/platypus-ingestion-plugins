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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ShardingStrategyFactoryTest {

  @Test
  public void testCreateWithNullConfig() {
    ShardingStrategy strategy = ShardingStrategyFactory.create(null);
    assertTrue(strategy instanceof NoShardingStrategy);
  }

  @Test
  public void testCreateWithEmptyConfig() {
    Map<String, Object> config = new HashMap<>();
    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof NoShardingStrategy);
  }

  @Test
  public void testCreateNoShardingStrategy() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "none");

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof NoShardingStrategy);
  }

  @Test
  public void testCreateNoShardingStrategyUppercase() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "NONE");

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof NoShardingStrategy);
  }

  @Test
  public void testCreateModuloStrategy() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "modulo");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", 30);
    moduloConfig.put("partition_field", "__internal_partition_id");
    config.put("modulo", moduloConfig);

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof ModuloShardingStrategy);

    ModuloShardingStrategy moduloStrategy = (ModuloShardingStrategy) strategy;
    assertEquals(30, moduloStrategy.getMaxShards());
    assertEquals("__internal_partition_id", moduloStrategy.getPartitionFieldName());
  }

  @Test
  public void testCreateModuloStrategyUppercase() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "MODULO");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", 30);
    moduloConfig.put("partition_field", "__internal_partition_id");
    config.put("modulo", moduloConfig);

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof ModuloShardingStrategy);
  }

  @Test
  public void testCreateModuloStrategyMissingMaxShards() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "modulo");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("partition_field", "__internal_partition_id");
    config.put("modulo", moduloConfig);

    try {
      ShardingStrategyFactory.create(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("modulo.max_shards"));
    }
  }

  @Test
  public void testCreateModuloStrategyMissingPartitionField() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "modulo");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", 30);
    config.put("modulo", moduloConfig);

    try {
      ShardingStrategyFactory.create(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("modulo.partition_field"));
    }
  }

  @Test
  public void testCreateGeoStrategy() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "geo");

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof GeoShardingStrategy);
  }

  @Test
  public void testCreateUnknownStrategy() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "unknown");

    try {
      ShardingStrategyFactory.create(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown sharding strategy"));
      assertTrue(e.getMessage().contains("unknown"));
      assertTrue(e.getMessage().contains("none, modulo, geo"));
    }
  }

  @Test
  public void testCreateWithNestedConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("strategy", "modulo");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", 15);
    moduloConfig.put("partition_field", "shard_id");
    config.put("modulo", moduloConfig);

    ShardingStrategy strategy = ShardingStrategyFactory.create(config);
    assertTrue(strategy instanceof ModuloShardingStrategy);

    ModuloShardingStrategy moduloStrategy = (ModuloShardingStrategy) strategy;
    assertEquals(15, moduloStrategy.getMaxShards());
    assertEquals("shard_id", moduloStrategy.getPartitionFieldName());
  }
}
