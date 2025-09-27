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

import java.util.Collections;
import java.util.List;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating ID sharding filters for Paimon tables.
 *
 * <p>Creates predicates of the form: primaryKey % shardingMax == serviceNumber where serviceNumber
 * is extracted from service names like "service-name-23".
 */
public class ShardingFilterBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingFilterBuilder.class);

  /**
   * Extract service number from service name.
   *
   * @param serviceName Service name like "nrtsearch-paimon-poc-23"
   * @return The number suffix (e.g., 23)
   * @throws IllegalArgumentException if service name doesn't end with a number
   */
  public static int extractServiceNumber(String serviceName) {
    // Find the last dash and extract number after it
    int lastDashIndex = serviceName.lastIndexOf('-');
    if (lastDashIndex == -1 || lastDashIndex == serviceName.length() - 1) {
      throw new IllegalArgumentException(
          "Service name '"
              + serviceName
              + "' does not end with a number (expected format: name-X where X is a number)");
    }

    String numberPart = serviceName.substring(lastDashIndex + 1);
    try {
      return Integer.parseInt(numberPart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Service name '"
              + serviceName
              + "' does not end with a valid number (expected format: name-X where X is a number)",
          e);
    }
  }

  /**
   * Build a sharding filter predicate for the given table.
   *
   * @param table The Paimon table
   * @param shardingMax Maximum number of shards (modulus)
   * @param serviceName Service name to extract shard number from
   * @return Predicate that filters rows for this service's shard
   * @throws IllegalArgumentException if table has no primary keys or service name is invalid
   */
  public static Predicate buildShardingFilter(Table table, int shardingMax, String serviceName) {
    int serviceNumber = extractServiceNumber(serviceName);
    return buildShardingFilter(table, shardingMax, serviceNumber);
  }

  /**
   * Build a sharding filter predicate for the given table.
   *
   * @param table The Paimon table
   * @param shardingMax Maximum number of shards (modulus)
   * @param serviceNumber The service number (remainder)
   * @return Predicate that filters rows for this service's shard
   * @throws IllegalArgumentException if table has no primary keys
   */
  public static Predicate buildShardingFilter(Table table, int shardingMax, int serviceNumber) {
    // Get primary key field (assume first primary key is the ID field for sharding)
    List<String> primaryKeys = table.primaryKeys();
    if (primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("Table has no primary keys, cannot apply ID sharding");
    }

    String idFieldName = primaryKeys.getFirst(); // Use first primary key as ID field
    LOGGER.info(
        "Building ID sharding filter: {} % {} == {} (field: {})",
        idFieldName, shardingMax, serviceNumber, idFieldName);

    // Create predicate for idField % shardingMax == serviceNumber
    RowType rowType = table.rowType();

    // Find ID field index and get field info
    int idFieldIndex = findFieldIndex(rowType, idFieldName);
    DataField idField = rowType.getFields().get(idFieldIndex);

    // Create custom modulo predicate
    ModuloEqual moduloPredicate = new ModuloEqual(shardingMax, serviceNumber);

    // Create LeafPredicate (literals are ignored by ModuloEqual)
    return new LeafPredicate(
        moduloPredicate, idField.type(), idFieldIndex, idFieldName, Collections.emptyList());
  }

  /**
   * Find the index of a field in the row type.
   *
   * @param rowType The table's row type
   * @param fieldName Name of the field to find
   * @return Field index
   * @throws IllegalArgumentException if field is not found
   */
  private static int findFieldIndex(RowType rowType, String fieldName) {
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      if (fieldName.equals(rowType.getFields().get(i).name())) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        "Field '" + fieldName + "' not found in table schema for ID sharding");
  }
}
