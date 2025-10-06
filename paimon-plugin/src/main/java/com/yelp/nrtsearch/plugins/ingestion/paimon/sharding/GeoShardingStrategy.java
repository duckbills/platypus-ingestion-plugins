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
 * Geographic sharding strategy (not yet implemented).
 *
 * <p>Placeholder for future geo-based sharding implementation.
 */
public class GeoShardingStrategy implements ShardingStrategy {

  @Override
  public ReadBuilder applySharding(ReadBuilder readBuilder, Table table, String serviceName) {
    throw new UnsupportedOperationException(
        "Geo sharding not yet implemented. Use sharding.strategy = 'none' or 'modulo'.");
  }

  @Override
  public void validateTable(Table table) {
    throw new UnsupportedOperationException(
        "Geo sharding not yet implemented. Use sharding.strategy = 'none' or 'modulo'.");
  }

  @Override
  public String getDescription() {
    return "Geo sharding (NOT YET IMPLEMENTED)";
  }
}
