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
package com.yelp.nrtsearch.plugins.ingestion.paimon.utils;

import java.util.Map;

/**
 * Helper for reading nested configuration maps with dot-separated paths.
 *
 * <p>Provides a simpler API similar to nrtsearch's YamlConfigReader for navigating nested
 * Map&lt;String, Object&gt; structures.
 */
public class ConfigHelper {

  public static String getString(Map<String, Object> config, String path) {
    Object value = navigate(config, path);
    if (value == null) {
      throw new IllegalArgumentException("Required configuration '" + path + "' is missing");
    }
    return value.toString();
  }

  public static String getString(Map<String, Object> config, String path, String defaultValue) {
    try {
      Object value = navigate(config, path);
      return value == null ? defaultValue : value.toString();
    } catch (IllegalArgumentException e) {
      return defaultValue;
    }
  }

  public static int getInteger(Map<String, Object> config, String path) {
    Object value = navigate(config, path);
    if (value == null) {
      throw new IllegalArgumentException("Required configuration '" + path + "' is missing");
    }
    if (value instanceof Integer) {
      return (Integer) value;
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for '" + path + "': " + value, e);
    }
  }

  public static Integer getInteger(Map<String, Object> config, String path, Integer defaultValue) {
    try {
      return getInteger(config, path);
    } catch (IllegalArgumentException e) {
      return defaultValue;
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getMap(Map<String, Object> config, String path) {
    Object value = navigate(config, path);
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("Value at '" + path + "' is not a map: " + value);
    }
    return (Map<String, Object>) value;
  }

  private static Object navigate(Map<String, Object> config, String path) {
    if (config == null || path == null || path.isEmpty()) {
      return null;
    }

    String[] parts = path.split("\\.");
    Object current = config;

    for (int i = 0; i < parts.length; i++) {
      if (!(current instanceof Map)) {
        throw new IllegalArgumentException(
            "Cannot navigate past non-map value in path '" + path + "'");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> currentMap = (Map<String, Object>) current;
      current = currentMap.get(parts[i]);

      if (current == null) {
        return null;
      }
    }

    return current;
  }
}
