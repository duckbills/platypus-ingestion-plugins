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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for ingestion plugins. This class provides a common way to handle
 * configuration parameters across different ingestion implementations.
 */
public class IngestionConfig {
  private final String indexName;
  private final int batchSize;
  private final Map<String, Object> properties;

  private IngestionConfig(Builder builder) {
    this.indexName = Preconditions.checkNotNull(builder.indexName, "indexName must not be null");
    this.batchSize = builder.batchSize;
    this.properties = new HashMap<>(builder.properties);
  }

  public String getIndexName() {
    return indexName;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Map<String, Object> getProperties() {
    return new HashMap<>(properties);
  }

  @SuppressWarnings("unchecked")
  public <T> T getProperty(String key, T defaultValue) {
    return (T) properties.getOrDefault(key, defaultValue);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String indexName;
    private int batchSize = 1000; // default batch size
    private final Map<String, Object> properties = new HashMap<>();

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setProperty(String key, Object value) {
      properties.put(key, value);
      return this;
    }

    public Builder setProperties(Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public IngestionConfig build() {
      return new IngestionConfig(this);
    }
  }
}
