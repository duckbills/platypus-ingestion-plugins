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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ConfigHelperTest {

  @Test
  public void testGetStringFromFlatConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("key", "value");

    assertEquals("value", ConfigHelper.getString(config, "key"));
  }

  @Test
  public void testGetStringFromNestedConfig() {
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> nested = new HashMap<>();
    nested.put("inner", "nested_value");
    config.put("outer", nested);

    assertEquals("nested_value", ConfigHelper.getString(config, "outer.inner"));
  }

  @Test
  public void testGetStringFromDeeplyNestedConfig() {
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> level1 = new HashMap<>();
    Map<String, Object> level2 = new HashMap<>();
    level2.put("deep_key", "deep_value");
    level1.put("level2", level2);
    config.put("level1", level1);

    assertEquals("deep_value", ConfigHelper.getString(config, "level1.level2.deep_key"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStringMissingKey() {
    Map<String, Object> config = new HashMap<>();
    ConfigHelper.getString(config, "missing");
  }

  @Test
  public void testGetStringWithDefault() {
    Map<String, Object> config = new HashMap<>();
    assertEquals("default", ConfigHelper.getString(config, "missing", "default"));
  }

  @Test
  public void testGetInteger() {
    Map<String, Object> config = new HashMap<>();
    config.put("count", 42);

    assertEquals(42, ConfigHelper.getInteger(config, "count"));
  }

  @Test
  public void testGetIntegerFromString() {
    Map<String, Object> config = new HashMap<>();
    config.put("count", "42");

    assertEquals(42, ConfigHelper.getInteger(config, "count"));
  }

  @Test
  public void testGetIntegerFromNestedConfig() {
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> nested = new HashMap<>();
    nested.put("count", 100);
    config.put("settings", nested);

    assertEquals(100, ConfigHelper.getInteger(config, "settings.count"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetIntegerMissingKey() {
    Map<String, Object> config = new HashMap<>();
    ConfigHelper.getInteger(config, "missing");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetIntegerInvalidFormat() {
    Map<String, Object> config = new HashMap<>();
    config.put("count", "not_a_number");

    ConfigHelper.getInteger(config, "count");
  }

  @Test
  public void testGetIntegerWithDefault() {
    Map<String, Object> config = new HashMap<>();
    assertEquals(Integer.valueOf(99), ConfigHelper.getInteger(config, "missing", 99));
  }

  @Test
  public void testGetMap() {
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> nested = new HashMap<>();
    nested.put("inner", "value");
    config.put("map", nested);

    Map<String, Object> result = ConfigHelper.getMap(config, "map");
    assertEquals(nested, result);
    assertEquals("value", result.get("inner"));
  }

  @Test
  public void testGetMapMissingKey() {
    Map<String, Object> config = new HashMap<>();
    assertNull(ConfigHelper.getMap(config, "missing"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMapNotAMap() {
    Map<String, Object> config = new HashMap<>();
    config.put("not_a_map", "string_value");

    ConfigHelper.getMap(config, "not_a_map");
  }

  @Test
  public void testNavigateWithNullConfig() {
    assertNull(ConfigHelper.getString(null, "any.path", null));
  }

  @Test
  public void testNavigateWithEmptyPath() {
    Map<String, Object> config = new HashMap<>();
    config.put("key", "value");

    assertNull(ConfigHelper.getString(config, "", null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNavigatePastNonMapValue() {
    Map<String, Object> config = new HashMap<>();
    config.put("key", "string_value");

    ConfigHelper.getString(config, "key.nested");
  }
}
