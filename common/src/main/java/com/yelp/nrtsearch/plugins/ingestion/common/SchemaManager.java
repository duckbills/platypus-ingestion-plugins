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
 * Interface for managing schema information between the source system and nrtsearch.
 * Implementations should handle schema conversion and registration with nrtsearch.
 */
public interface SchemaManager {

  /**
   * Initialize the schema manager with configuration parameters.
   *
   * @param config Configuration parameters for schema management
   */
  void initialize(Map<String, Object> config);

  /**
   * Get the current schema version from the source system.
   *
   * @return A string representing the current schema version
   */
  String getCurrentSchemaVersion();

  /**
   * Check if a schema needs to be created or updated in nrtsearch.
   *
   * @param indexName The name of the nrtsearch index
   * @return true if schema needs to be created/updated, false otherwise
   */
  boolean requiresSchemaUpdate(String indexName);

  /**
   * Create or update the schema in nrtsearch based on the source system's schema.
   *
   * @param indexName The name of the nrtsearch index
   * @throws Exception if schema creation/update fails
   */
  void createOrUpdateSchema(String indexName) throws Exception;
}
