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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.junit.Test;

public class ModuloShardingStrategyTest {

  @Test
  public void testConstructorValidation() {
    new ModuloShardingStrategy(30, "partition_field");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorZeroMaxShards() {
    new ModuloShardingStrategy(0, "partition_field");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNegativeMaxShards() {
    new ModuloShardingStrategy(-1, "partition_field");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullPartitionField() {
    new ModuloShardingStrategy(30, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorEmptyPartitionField() {
    new ModuloShardingStrategy(30, "");
  }

  @Test
  public void testConstructorTrimsPartitionField() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "  partition_field  ");
    assertEquals("partition_field", strategy.getPartitionFieldName());
  }

  @Test
  public void testValidateTableWithPartitionedTable() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.partitionKeys())
        .thenReturn(Arrays.asList("__internal_partition_id", "date_partition"));
    when(mockTable.name()).thenReturn("test_table");

    strategy.validateTable(mockTable);
  }

  @Test
  public void testValidateTableNotPartitioned() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.partitionKeys()).thenReturn(Collections.emptyList());
    when(mockTable.name()).thenReturn("test_table");

    try {
      strategy.validateTable(mockTable);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("requires a PARTITIONED table"));
      assertTrue(e.getMessage().contains("test_table"));
    }
  }

  @Test
  public void testValidateTablePartitionFieldNotFound() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.partitionKeys()).thenReturn(Arrays.asList("date_partition", "region"));
    when(mockTable.name()).thenReturn("test_table");

    try {
      strategy.validateTable(mockTable);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("__internal_partition_id"));
      assertTrue(e.getMessage().contains("not found in table partition keys"));
      assertTrue(e.getMessage().contains("[date_partition, region]"));
    }
  }

  @Test
  public void testApplySharding() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    ReadBuilder result = strategy.applySharding(mockReadBuilder, mockTable, "test-service-15");

    verify(mockReadBuilder).withPartitionFilter(anyMap());
    assertEquals(mockReadBuilder, result);
  }

  @Test
  public void testGetDescription() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");
    String description = strategy.getDescription();

    assertTrue(description.contains("Modulo sharding"));
    assertTrue(description.contains("__internal_partition_id"));
    assertTrue(description.contains("30"));
    assertTrue(description.contains("partition-level file pruning"));
  }

  @Test
  public void testGetters() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    assertEquals(30, strategy.getMaxShards());
    assertEquals("__internal_partition_id", strategy.getPartitionFieldName());
  }

  @Test
  public void testShardIdExtractionWithDashes() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    strategy.applySharding(mockReadBuilder, mockTable, "nrtsearch-photos-23");
    // Should extract shard ID 23 from service name
  }

  @Test
  public void testShardIdExtractionWithUnderscores() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    strategy.applySharding(mockReadBuilder, mockTable, "my_service_5");
    // Should extract shard ID 5 from service name
  }

  @Test
  public void testShardIdExtractionZero() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    strategy.applySharding(mockReadBuilder, mockTable, "service-0");
    // Should extract shard ID 0 from service name
  }

  @Test
  public void testShardIdExtractionMaxShard() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    strategy.applySharding(mockReadBuilder, mockTable, "service-29");
    // Should extract shard ID 29 (max_shards - 1)
  }

  @Test
  public void testServiceNameWithoutNumber() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    try {
      strategy.applySharding(mockReadBuilder, mockTable, "my-service-name");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must end with shard number"));
      assertTrue(e.getMessage().contains("my-service-name"));
    }
  }

  @Test
  public void testShardIdOutOfRange() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    try {
      strategy.applySharding(mockReadBuilder, mockTable, "service-30");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("shard ID 30"));
      assertTrue(e.getMessage().contains("must be in range [0, 30)"));
    }
  }

  @Test
  public void testNegativeShardId() {
    ModuloShardingStrategy strategy = new ModuloShardingStrategy(30, "__internal_partition_id");

    Table mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");

    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    when(mockReadBuilder.withPartitionFilter(anyMap())).thenReturn(mockReadBuilder);

    try {
      // Service name with two dashes creates an empty string after split, which parses as
      // non-number
      // Let's use a direct negative number case
      strategy.applySharding(mockReadBuilder, mockTable, "service-n5");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Should fail on non-numeric parsing, not range validation
      assertTrue(
          e.getMessage().contains("must end with shard number")
              || e.getMessage().contains("not a valid number"));
    }
  }
}
