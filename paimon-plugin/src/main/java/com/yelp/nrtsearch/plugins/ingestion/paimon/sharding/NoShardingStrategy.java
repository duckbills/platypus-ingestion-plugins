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
 * No sharding strategy - processes all data from the table.
 *
 * <p>This strategy is used when:
 *
 * <ul>
 *   <li>Single nrtSearch instance deployment (no horizontal scaling)
 *   <li>Small dataset that doesn't require sharding
 *   <li>Testing/development environments
 * </ul>
 *
 * <p>The ReadBuilder is returned unchanged - no filtering applied.
 */
public class NoShardingStrategy implements ShardingStrategy {

  @Override
  public ReadBuilder applySharding(ReadBuilder readBuilder, Table table, String serviceName) {
    // No filtering needed - process all data
    return readBuilder;
  }

  @Override
  public void validateTable(Table table) {
    // Any table is valid for no sharding (partitioned or unpartitioned)
  }

  @Override
  public String getDescription() {
    return "No sharding - processing all data";
  }
}
