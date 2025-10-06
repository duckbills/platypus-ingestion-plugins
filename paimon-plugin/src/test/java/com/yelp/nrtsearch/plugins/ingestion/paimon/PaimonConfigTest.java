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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class PaimonConfigTest {

  @Test
  public void testRequiredConfiguration() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals("test_db", paimonConfig.getDatabaseName());
    assertEquals("test_table", paimonConfig.getTableName());
    assertEquals("test_db.test_table", paimonConfig.getTablePath());
    assertEquals("test_index", paimonConfig.getTargetIndexName());
    assertEquals("/tmp/paimon", paimonConfig.getWarehousePath());
  }

  @Test
  public void testOptionalConfiguration() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");
    config.put("worker.threads", "8");
    config.put("batch.size", "2000");

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals(8, paimonConfig.getWorkerThreads());
    assertEquals(2000, paimonConfig.getBatchSize());
    assertEquals(1000L, paimonConfig.getPollTimeoutMs()); // default
    assertEquals(30000L, paimonConfig.getScanIntervalMs()); // default
  }

  @Test
  public void testFieldMapping() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("paimon_field", "nrtsearch_field");
    config.put("field.mapping", fieldMapping);

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals(fieldMapping, paimonConfig.getFieldMapping());
  }

  @Test
  public void testNoFieldMapping() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertNull(paimonConfig.getFieldMapping());
  }

  @Test
  public void testDatabaseNameWithDots() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "yelp.business.data");
    config.put("table.name", "reviews");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals("yelp.business.data", paimonConfig.getDatabaseName());
    assertEquals("reviews", paimonConfig.getTableName());
    assertEquals("yelp.business.data.reviews", paimonConfig.getTablePath());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingRequiredConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    // Missing table.name, target.index.name and warehouse.path

    new PaimonConfig(config);
  }

  @Test
  public void testShardingConfigNone() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    Map<String, Object> shardingConfig = new HashMap<>();
    shardingConfig.put("strategy", "none");
    config.put("sharding", shardingConfig);

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals(shardingConfig, paimonConfig.getShardingConfig());
  }

  @Test
  public void testShardingConfigModulo() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    Map<String, Object> moduloConfig = new HashMap<>();
    moduloConfig.put("max_shards", 30);
    moduloConfig.put("partition_field", "__internal_partition_id");

    Map<String, Object> shardingConfig = new HashMap<>();
    shardingConfig.put("strategy", "modulo");
    shardingConfig.put("modulo", moduloConfig);
    config.put("sharding", shardingConfig);

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertEquals(shardingConfig, paimonConfig.getShardingConfig());
    assertEquals("modulo", paimonConfig.getShardingConfig().get("strategy"));

    @SuppressWarnings("unchecked")
    Map<String, Object> retrievedModuloConfig =
        (Map<String, Object>) paimonConfig.getShardingConfig().get("modulo");
    assertEquals(30, retrievedModuloConfig.get("max_shards"));
    assertEquals("__internal_partition_id", retrievedModuloConfig.get("partition_field"));
  }

  @Test
  public void testNoShardingConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("target.index.name", "test_index");
    config.put("warehouse.path", "/tmp/paimon");

    PaimonConfig paimonConfig = new PaimonConfig(config);

    assertNull(paimonConfig.getShardingConfig());
  }
}
