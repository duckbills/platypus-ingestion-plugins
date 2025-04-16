/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.plugins.ingestion.common;

import java.util.Map;

/**
 * Interface for managing offsets during data ingestion. Implementations should handle storing and
 * retrieving offsets to enable resuming ingestion after crashes or restarts.
 */
public interface OffsetManager {

  /**
   * Initialize the offset manager with configuration parameters.
   *
   * @param config Configuration parameters for offset management
   */
  void initialize(Map<String, Object> config);

  /**
   * Get the last saved offset for a specific partition.
   *
   * @param partition Identifier for the data partition
   * @return The last saved offset, or null if no offset was saved
   */
  String getLastOffset(String partition);

  /**
   * Save the current offset for a specific partition.
   *
   * @param partition Identifier for the data partition
   * @param offset The offset to save
   */
  void saveOffset(String partition, String offset);

  /**
   * Commit all pending offset saves to ensure durability. This method should be called periodically
   * to ensure offsets are persisted.
   */
  void commitOffsets();

  /** Clean up any resources used by the offset manager. */
  void close();
}
