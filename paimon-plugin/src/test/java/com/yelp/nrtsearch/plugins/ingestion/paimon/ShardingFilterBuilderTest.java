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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ShardingFilterBuilderTest {

  @Test
  public void testExtractServiceNumber_ValidServiceName() {
    assertEquals(23, ShardingFilterBuilder.extractServiceNumber("nrtsearch-paimon-poc-23"));
    assertEquals(1, ShardingFilterBuilder.extractServiceNumber("service-1"));
    assertEquals(999, ShardingFilterBuilder.extractServiceNumber("my-long-service-name-999"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractServiceNumber_NoNumber() {
    ShardingFilterBuilder.extractServiceNumber("service-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractServiceNumber_NoDash() {
    ShardingFilterBuilder.extractServiceNumber("servicename23");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractServiceNumber_EndsWithDash() {
    ShardingFilterBuilder.extractServiceNumber("service-name-");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractServiceNumber_InvalidNumber() {
    ShardingFilterBuilder.extractServiceNumber("service-name-abc");
  }

  @Test
  public void testBuildShardingFilter_WithValidTable() {
    // Create mock table with primary key
    Table mockTable = mock(Table.class);
    List<String> primaryKeys = Arrays.asList("photo_id", "business_id");
    when(mockTable.primaryKeys()).thenReturn(primaryKeys);

    // Create mock row type with photo_id field
    RowType mockRowType = mock(RowType.class);
    DataField photoIdField = new DataField(0, "photo_id", DataTypes.BIGINT());
    DataField businessIdField = new DataField(1, "business_id", DataTypes.INT());
    List<DataField> fields = Arrays.asList(photoIdField, businessIdField);

    when(mockRowType.getFieldCount()).thenReturn(2);
    when(mockRowType.getFields()).thenReturn(fields);
    when(mockTable.rowType()).thenReturn(mockRowType);

    // Test building filter
    Predicate filter = ShardingFilterBuilder.buildShardingFilter(mockTable, 30, "service-23");

    assertNotNull(filter);
    // Verify the table methods were called
    verify(mockTable).primaryKeys();
    verify(mockTable).rowType();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildShardingFilter_NoPrimaryKeys() {
    // Create mock table with no primary keys
    Table mockTable = mock(Table.class);
    when(mockTable.primaryKeys()).thenReturn(Collections.emptyList());

    ShardingFilterBuilder.buildShardingFilter(mockTable, 30, "service-23");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildShardingFilter_PrimaryKeyNotInSchema() {
    // Create mock table with primary key that doesn't exist in schema
    Table mockTable = mock(Table.class);
    List<String> primaryKeys = Arrays.asList("missing_field");
    when(mockTable.primaryKeys()).thenReturn(primaryKeys);

    // Create mock row type without the primary key field
    RowType mockRowType = mock(RowType.class);
    DataField otherField = new DataField(0, "other_field", DataTypes.BIGINT());
    List<DataField> fields = Arrays.asList(otherField);

    when(mockRowType.getFieldCount()).thenReturn(1);
    when(mockRowType.getFields()).thenReturn(fields);
    when(mockTable.rowType()).thenReturn(mockRowType);

    ShardingFilterBuilder.buildShardingFilter(mockTable, 30, 23);
  }

  @Test
  public void testBuildShardingFilter_WithServiceName() {
    // Create mock table
    Table mockTable = mock(Table.class);
    List<String> primaryKeys = Arrays.asList("id");
    when(mockTable.primaryKeys()).thenReturn(primaryKeys);

    // Create mock row type
    RowType mockRowType = mock(RowType.class);
    DataField idField = new DataField(0, "id", DataTypes.BIGINT());
    List<DataField> fields = Arrays.asList(idField);

    when(mockRowType.getFieldCount()).thenReturn(1);
    when(mockRowType.getFields()).thenReturn(fields);
    when(mockTable.rowType()).thenReturn(mockRowType);

    // Test building filter with service name parsing
    Predicate filter = ShardingFilterBuilder.buildShardingFilter(mockTable, 10, "my-service-7");

    assertNotNull(filter);
  }
}
