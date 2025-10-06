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
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.junit.Test;

public class NoShardingStrategyTest {

  @Test
  public void testApplySharding() {
    NoShardingStrategy strategy = new NoShardingStrategy();
    ReadBuilder mockReadBuilder = mock(ReadBuilder.class);
    Table mockTable = mock(Table.class);

    ReadBuilder result = strategy.applySharding(mockReadBuilder, mockTable, "test-service");

    assertSame(mockReadBuilder, result);
  }

  @Test
  public void testValidateTable() {
    NoShardingStrategy strategy = new NoShardingStrategy();
    Table mockTable = mock(Table.class);

    strategy.validateTable(mockTable);
  }

  @Test
  public void testGetDescription() {
    NoShardingStrategy strategy = new NoShardingStrategy();
    assertEquals("No sharding - processing all data", strategy.getDescription());
  }
}
