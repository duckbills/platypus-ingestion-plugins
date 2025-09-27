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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.plugins.ingestion.paimon.PaimonToAddDocumentConverter.UnrecoverableConversionException;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.Before;
import org.junit.Test;

/**
 * Comprehensive unit tests for PaimonToAddDocumentConverter covering all Paimon data types and
 * their conversion to NRTSearch AddDocumentRequest format.
 */
public class PaimonToAddDocumentConverterTest {

  private PaimonToAddDocumentConverter converter;
  private PaimonConfig config;
  private static final String INDEX_NAME = "test_index";

  @Before
  public void setUp() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("database.name", "test_db");
    configMap.put("table.name", "test_table");
    configMap.put("target.index.name", INDEX_NAME);
    configMap.put("warehouse.path", "s3://test-bucket/test-warehouse");

    config = new PaimonConfig(configMap);
    converter = new PaimonToAddDocumentConverter(config);
  }

  @Test
  public void testPrimitiveTypes() throws UnrecoverableConversionException {
    // Create row type with all primitive types
    RowType rowType =
        RowType.of(
            new DataField(0, "booleanField", new BooleanType()),
            new DataField(1, "tinyintField", new TinyIntType()),
            new DataField(2, "smallintField", new SmallIntType()),
            new DataField(3, "intField", new IntType()),
            new DataField(4, "bigintField", new BigIntType()),
            new DataField(5, "floatField", new FloatType()),
            new DataField(6, "doubleField", new DoubleType()),
            new DataField(7, "charField", new CharType(10)),
            new DataField(8, "varcharField", new VarCharType(50)));

    converter.setRowType(rowType);

    // Create test row with sample data
    GenericRow row = new GenericRow(9);
    row.setField(0, true);
    row.setField(1, (byte) 127);
    row.setField(2, (short) 32767);
    row.setField(3, 2147483647);
    row.setField(4, 9223372036854775807L);
    row.setField(5, 3.14f);
    row.setField(6, 2.718281828);
    row.setField(7, BinaryString.fromString("char_test"));
    row.setField(8, BinaryString.fromString("varchar_test"));

    AddDocumentRequest request = converter.convertRowToDocument(row);

    assertEquals(INDEX_NAME, request.getIndexName());
    assertEquals("true", getSingleValue(request, "booleanField"));
    assertEquals("127", getSingleValue(request, "tinyintField"));
    assertEquals("32767", getSingleValue(request, "smallintField"));
    assertEquals("2147483647", getSingleValue(request, "intField"));
    assertEquals("9223372036854775807", getSingleValue(request, "bigintField"));
    assertEquals("3.14", getSingleValue(request, "floatField"));
    assertEquals("2.718281828", getSingleValue(request, "doubleField"));
    assertEquals("char_test", getSingleValue(request, "charField"));
    assertEquals("varchar_test", getSingleValue(request, "varcharField"));
  }

  @Test
  public void testDecimalType() throws UnrecoverableConversionException {
    RowType rowType = RowType.of(new DataField(0, "decimalField", new DecimalType(10, 2)));
    converter.setRowType(rowType);

    GenericRow row = new GenericRow(1);
    row.setField(0, Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2));

    AddDocumentRequest request = converter.convertRowToDocument(row);
    assertEquals("123.45", getSingleValue(request, "decimalField"));
  }

  @Test
  public void testDateAndTimeTypes() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "dateField", new DateType()),
            new DataField(1, "timeField", new TimeType()),
            new DataField(2, "timestampField", new TimestampType()));
    converter.setRowType(rowType);

    GenericRow row = new GenericRow(3);
    row.setField(0, 19723); // Days since epoch (2024-01-01)
    row.setField(1, 43200000); // Milliseconds since midnight (12:00:00)
    row.setField(2, Timestamp.fromEpochMillis(1704110400000L)); // 2024-01-01 12:00:00 UTC

    AddDocumentRequest request = converter.convertRowToDocument(row);
    assertEquals("19723", getSingleValue(request, "dateField"));
    assertEquals("43200000", getSingleValue(request, "timeField"));
    assertEquals("1704110400000", getSingleValue(request, "timestampField"));
  }

  @Test
  public void testBinaryTypes() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "binaryField", new BinaryType(10)),
            new DataField(1, "varbinaryField", new VarBinaryType(50)));
    converter.setRowType(rowType);

    byte[] binaryData = "test_data".getBytes();
    GenericRow row = new GenericRow(2);
    row.setField(0, binaryData);
    row.setField(1, binaryData);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    // Base64 encoded version of "test_data"
    String expectedBase64 = java.util.Base64.getEncoder().encodeToString(binaryData);
    assertEquals(expectedBase64, getSingleValue(request, "binaryField"));
    assertEquals(expectedBase64, getSingleValue(request, "varbinaryField"));
  }

  @Test
  public void testArrayType() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(new DataField(0, "stringArrayField", new ArrayType(new VarCharType(50))));
    converter.setRowType(rowType);

    // Create array with string elements
    Object[] arrayData =
        new Object[] {
          BinaryString.fromString("item1"),
          BinaryString.fromString("item2"),
          BinaryString.fromString("item3")
        };
    GenericArray array = new GenericArray(arrayData);

    GenericRow row = new GenericRow(1);
    row.setField(0, array);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    // Arrays are converted to JSON string representation
    String arrayValue = getSingleValue(request, "stringArrayField");
    assertTrue(arrayValue.contains("item1"));
    assertTrue(arrayValue.contains("item2"));
    assertTrue(arrayValue.contains("item3"));
    assertTrue(arrayValue.startsWith("["));
    assertTrue(arrayValue.endsWith("]"));
  }

  @Test
  public void testDoubleArrayAndStringArrayConversion() throws UnrecoverableConversionException {
    // Test common array types: embeddings (doubles) and tags (strings)
    RowType rowType =
        RowType.of(
            new DataField(0, "embedding_vector", new ArrayType(new DoubleType())),
            new DataField(1, "tag_list", new ArrayType(new VarCharType(255))));
    converter.setRowType(rowType);

    // Create embedding_vector: array of doubles (typical ML embedding)
    Object[] embeddingData = new Object[] {-0.12345, 0.67890, 1.23456, -2.34567, 0.0, 3.14159};
    GenericArray embeddingArray = new GenericArray(embeddingData);

    // Create tag_list: array of strings with international characters
    Object[] tagsData =
        new Object[] {
          BinaryString.fromString("machine_learning"),
          BinaryString.fromString("データ"),
          BinaryString.fromString("测试")
        };
    GenericArray tagsArray = new GenericArray(tagsData);

    GenericRow row = new GenericRow(2);
    row.setField(0, embeddingArray);
    row.setField(1, tagsArray);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    // Test embedding_vector conversion - doubles without quotes
    String embeddingValue = getSingleValue(request, "embedding_vector");
    assertEquals("[-0.12345,0.6789,1.23456,-2.34567,0.0,3.14159]", embeddingValue);

    // Test tag_list conversion - strings with quotes
    String tagsValue = getSingleValue(request, "tag_list");
    assertEquals("[\"machine_learning\",\"データ\",\"测试\"]", tagsValue);
  }

  @Test
  public void testArrayWithNullElements() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "doubleArrayWithNulls", new ArrayType(new DoubleType().nullable())),
            new DataField(
                1, "stringArrayWithNulls", new ArrayType(new VarCharType(50).nullable())));
    converter.setRowType(rowType);

    // Create arrays with null elements
    Object[] doubleArrayData = new Object[] {1.5, null, 2.5};
    Object[] stringArrayData =
        new Object[] {BinaryString.fromString("first"), null, BinaryString.fromString("third")};

    GenericArray doubleArray = new GenericArray(doubleArrayData);
    GenericArray stringArray = new GenericArray(stringArrayData);

    GenericRow row = new GenericRow(2);
    row.setField(0, doubleArray);
    row.setField(1, stringArray);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    // Test null handling in arrays
    String doubleValue = getSingleValue(request, "doubleArrayWithNulls");
    assertEquals("[1.5,null,2.5]", doubleValue);

    String stringValue = getSingleValue(request, "stringArrayWithNulls");
    assertEquals("[\"first\",null,\"third\"]", stringValue);
  }

  @Test
  public void testArrayWithSpecialCharacters() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "stringArrayWithSpecialChars", new ArrayType(new VarCharType(255))));
    converter.setRowType(rowType);

    // Create array with special characters that need JSON escaping
    Object[] arrayData =
        new Object[] {
          BinaryString.fromString("quote\"test"),
          BinaryString.fromString("newline\ntest"),
          BinaryString.fromString("backslash\\test"),
          BinaryString.fromString("tab\ttest")
        };
    GenericArray array = new GenericArray(arrayData);

    GenericRow row = new GenericRow(1);
    row.setField(0, array);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    String arrayValue = getSingleValue(request, "stringArrayWithSpecialChars");
    assertEquals(
        "[\"quote\\\"test\",\"newline\\ntest\",\"backslash\\\\test\",\"tab\\ttest\"]", arrayValue);
  }

  @Test
  public void testMapType() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "mapField", new MapType(new VarCharType(50), new VarCharType(50))));
    converter.setRowType(rowType);

    // Create map with string keys and values
    Map<Object, Object> mapData = new HashMap<>();
    mapData.put(BinaryString.fromString("key1"), BinaryString.fromString("value1"));
    mapData.put(BinaryString.fromString("key2"), BinaryString.fromString("value2"));
    GenericMap map = new GenericMap(mapData);

    GenericRow row = new GenericRow(1);
    row.setField(0, map);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    // Maps are converted to JSON string representation
    String mapValue = getSingleValue(request, "mapField");
    assertTrue(mapValue.contains("key1"));
    assertTrue(mapValue.contains("value1"));
    assertTrue(mapValue.contains("key2"));
    assertTrue(mapValue.contains("value2"));
    assertTrue(mapValue.startsWith("{"));
    assertTrue(mapValue.endsWith("}"));
  }

  @Test
  public void testNestedRowType() throws UnrecoverableConversionException {
    // Create nested row type
    RowType nestedRowType =
        RowType.of(
            new DataField(0, "innerField", new VarCharType(50)),
            new DataField(1, "innerNumber", new IntType()));

    RowType rowType =
        RowType.of(
            new DataField(0, "id", new IntType()), new DataField(1, "nested", nestedRowType));
    converter.setRowType(rowType);

    // Create nested row
    GenericRow nestedRecord = new GenericRow(2);
    nestedRecord.setField(0, BinaryString.fromString("nested_value"));
    nestedRecord.setField(1, 42);

    GenericRow row = new GenericRow(2);
    row.setField(0, 123);
    row.setField(1, nestedRecord);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    assertEquals("123", getSingleValue(request, "id"));
    // Nested rows are converted to JSON string representation
    String nestedValue = getSingleValue(request, "nested");
    assertTrue(nestedValue.contains("nested"));
    assertTrue(nestedValue.contains("row"));
  }

  @Test
  public void testNullValues() throws UnrecoverableConversionException {
    RowType rowType =
        RowType.of(
            new DataField(0, "stringField", new VarCharType(50)),
            new DataField(1, "nullField", new VarCharType(50)),
            new DataField(2, "intField", new IntType()));
    converter.setRowType(rowType);

    GenericRow row = new GenericRow(3);
    row.setField(0, BinaryString.fromString("not_null"));
    row.setField(1, null); // null value
    row.setField(2, 42);

    AddDocumentRequest request = converter.convertRowToDocument(row);

    assertEquals("not_null", getSingleValue(request, "stringField"));
    assertEquals("42", getSingleValue(request, "intField"));
    // Null fields should not be present in the request
    assertFalse(request.getFieldsMap().containsKey("nullField"));
  }

  @Test
  public void testFieldMapping() throws UnrecoverableConversionException {
    // Create config with field mapping
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("database.name", "test_db");
    configMap.put("table.name", "test_table");
    configMap.put("target.index.name", INDEX_NAME);
    configMap.put("warehouse.path", "s3://test-bucket/test-warehouse");

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("old_field_name", "new_field_name");
    configMap.put("field.mapping", fieldMapping);

    PaimonConfig configWithMapping = new PaimonConfig(configMap);
    PaimonToAddDocumentConverter converterWithMapping =
        new PaimonToAddDocumentConverter(configWithMapping);

    RowType rowType =
        RowType.of(
            new DataField(0, "old_field_name", new VarCharType(50)),
            new DataField(1, "unchanged_field", new VarCharType(50)));
    converterWithMapping.setRowType(rowType);

    GenericRow row = new GenericRow(2);
    row.setField(0, BinaryString.fromString("mapped_value"));
    row.setField(1, BinaryString.fromString("unchanged_value"));

    AddDocumentRequest request = converterWithMapping.convertRowToDocument(row);

    // Field should be mapped to new name
    assertEquals("mapped_value", getSingleValue(request, "new_field_name"));
    assertEquals("unchanged_value", getSingleValue(request, "unchanged_field"));
    // Old field name should not exist
    assertFalse(request.getFieldsMap().containsKey("old_field_name"));
  }

  @Test(expected = IllegalStateException.class)
  public void testConvertWithoutRowType() throws UnrecoverableConversionException {
    // Should throw IllegalStateException when RowType is not set
    GenericRow row = new GenericRow(1);
    converter.convertRowToDocument(row);
  }

  @Test
  public void testUnsupportedTypeHandling() throws UnrecoverableConversionException {
    // Note: All current Paimon types are supported, but this tests the fallback behavior
    // if an unsupported type is encountered in the future
    RowType rowType = RowType.of(new DataField(0, "supportedField", new VarCharType(50)));
    converter.setRowType(rowType);

    GenericRow row = new GenericRow(1);
    row.setField(0, BinaryString.fromString("test_value"));

    AddDocumentRequest request = converter.convertRowToDocument(row);
    assertEquals("test_value", getSingleValue(request, "supportedField"));
  }

  @Test
  public void testEmptyRow() throws UnrecoverableConversionException {
    RowType rowType = RowType.of(); // Empty row type
    converter.setRowType(rowType);

    GenericRow row = new GenericRow(0);
    AddDocumentRequest request = converter.convertRowToDocument(row);

    assertEquals(INDEX_NAME, request.getIndexName());
    assertTrue(request.getFieldsMap().isEmpty());
  }

  @Test
  public void testFieldDropFunctionality() throws UnrecoverableConversionException {
    // Create a PaimonConfig with field drop prefixes
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("warehouse.path", "file:///tmp/test");
    config.put("target.index.name", INDEX_NAME);
    config.put("field.drop.prefixes", java.util.Arrays.asList("__internal__", "__debug_"));

    PaimonConfig configWithDrop = new PaimonConfig(config);
    PaimonToAddDocumentConverter converterWithDrop =
        new PaimonToAddDocumentConverter(configWithDrop);

    // Create a schema with fields that should and shouldn't be dropped
    RowType rowType =
        RowType.of(
            new DataField(0, "user_id", new VarCharType(50)),
            new DataField(1, "__internal__metadata", new VarCharType(100)),
            new DataField(2, "title", new VarCharType(200)),
            new DataField(3, "__debug_trace", new VarCharType(500)),
            new DataField(4, "content", new VarCharType(1000)));
    converterWithDrop.setRowType(rowType);

    // Create row with data for all fields
    GenericRow row = new GenericRow(5);
    row.setField(0, BinaryString.fromString("user123"));
    row.setField(1, BinaryString.fromString("internal_data"));
    row.setField(2, BinaryString.fromString("Test Title"));
    row.setField(3, BinaryString.fromString("debug_info"));
    row.setField(4, BinaryString.fromString("Test Content"));

    // Convert row to document
    AddDocumentRequest request = converterWithDrop.convertRowToDocument(row);

    // Verify that only non-dropped fields are present
    assertEquals(INDEX_NAME, request.getIndexName());
    assertTrue("Should contain user_id", request.getFieldsMap().containsKey("user_id"));
    assertTrue("Should contain title", request.getFieldsMap().containsKey("title"));
    assertTrue("Should contain content", request.getFieldsMap().containsKey("content"));

    // Verify that dropped fields are NOT present
    assertFalse(
        "Should NOT contain __internal__metadata",
        request.getFieldsMap().containsKey("__internal__metadata"));
    assertFalse(
        "Should NOT contain __debug_trace", request.getFieldsMap().containsKey("__debug_trace"));

    // Verify we have exactly 3 fields (not 5)
    assertEquals("Should have 3 fields after dropping 2", 3, request.getFieldsMap().size());

    // Verify field values are correct
    assertEquals("user123", getSingleValue(request, "user_id"));
    assertEquals("Test Title", getSingleValue(request, "title"));
    assertEquals("Test Content", getSingleValue(request, "content"));
  }

  @Test
  public void testNoDropPrefixesConfigured() throws UnrecoverableConversionException {
    // Create config without field drop prefixes
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("warehouse.path", "file:///tmp/test");
    config.put("target.index.name", INDEX_NAME);
    // No field.drop.prefixes configured

    PaimonConfig configNoDrop = new PaimonConfig(config);
    PaimonToAddDocumentConverter converterNoDrop = new PaimonToAddDocumentConverter(configNoDrop);

    // Create schema with internal fields
    RowType rowType =
        RowType.of(
            new DataField(0, "user_id", new VarCharType(50)),
            new DataField(1, "__internal__metadata", new VarCharType(100)));
    converterNoDrop.setRowType(rowType);

    // Create row with data
    GenericRow row = new GenericRow(2);
    row.setField(0, BinaryString.fromString("user123"));
    row.setField(1, BinaryString.fromString("internal_data"));

    // Convert row to document
    AddDocumentRequest request = converterNoDrop.convertRowToDocument(row);

    // Verify that all fields are present (including internal ones)
    assertTrue("Should contain user_id", request.getFieldsMap().containsKey("user_id"));
    assertTrue(
        "Should contain __internal__metadata",
        request.getFieldsMap().containsKey("__internal__metadata"));
    assertEquals("Should have all 2 fields", 2, request.getFieldsMap().size());

    // Verify field values
    assertEquals("user123", getSingleValue(request, "user_id"));
    assertEquals("internal_data", getSingleValue(request, "__internal__metadata"));
  }

  @Test
  public void testEmptyDropPrefixesList() throws UnrecoverableConversionException {
    // Create config with empty field drop prefixes list
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("warehouse.path", "file:///tmp/test");
    config.put("target.index.name", INDEX_NAME);
    config.put("field.drop.prefixes", java.util.Collections.emptyList());

    PaimonConfig configEmptyDrop = new PaimonConfig(config);
    PaimonToAddDocumentConverter converterEmptyDrop =
        new PaimonToAddDocumentConverter(configEmptyDrop);

    // Create schema with internal fields
    RowType rowType = RowType.of(new DataField(0, "__internal__test", new VarCharType(50)));
    converterEmptyDrop.setRowType(rowType);

    // Create row with data
    GenericRow row = new GenericRow(1);
    row.setField(0, BinaryString.fromString("test_value"));

    // Convert row to document
    AddDocumentRequest request = converterEmptyDrop.convertRowToDocument(row);

    // Verify that field is present (empty drop list means no dropping)
    assertTrue(
        "Should contain __internal__test", request.getFieldsMap().containsKey("__internal__test"));
    assertEquals("Should have 1 field", 1, request.getFieldsMap().size());
    assertEquals("test_value", getSingleValue(request, "__internal__test"));
  }

  @Test
  public void testFieldDropWithFieldMapping() throws UnrecoverableConversionException {
    // Create config with both field drop and field mapping
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("warehouse.path", "file:///tmp/test");
    config.put("target.index.name", INDEX_NAME);
    config.put("field.drop.prefixes", java.util.Arrays.asList("__temp_"));

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("old_name", "new_name");
    config.put("field.mapping", fieldMapping);

    PaimonConfig configWithBoth = new PaimonConfig(config);
    PaimonToAddDocumentConverter converterWithBoth =
        new PaimonToAddDocumentConverter(configWithBoth);

    // Create schema with mix of fields: drop, map, and keep as-is
    RowType rowType =
        RowType.of(
            new DataField(0, "old_name", new VarCharType(50)), // Should be mapped to "new_name"
            new DataField(1, "__temp_data", new VarCharType(100)), // Should be dropped
            new DataField(2, "keep_as_is", new VarCharType(200)) // Should keep original name
            );
    converterWithBoth.setRowType(rowType);

    // Create row with data
    GenericRow row = new GenericRow(3);
    row.setField(0, BinaryString.fromString("mapped_value"));
    row.setField(1, BinaryString.fromString("dropped_value"));
    row.setField(2, BinaryString.fromString("original_value"));

    // Convert row to document
    AddDocumentRequest request = converterWithBoth.convertRowToDocument(row);

    // Verify field mapping and dropping worked correctly
    assertTrue("Should contain new_name (mapped)", request.getFieldsMap().containsKey("new_name"));
    assertTrue("Should contain keep_as_is", request.getFieldsMap().containsKey("keep_as_is"));
    assertFalse(
        "Should NOT contain old_name (original)", request.getFieldsMap().containsKey("old_name"));
    assertFalse(
        "Should NOT contain __temp_data (dropped)",
        request.getFieldsMap().containsKey("__temp_data"));

    // Verify we have exactly 2 fields (1 dropped, 1 mapped)
    assertEquals("Should have 2 fields", 2, request.getFieldsMap().size());

    // Verify field values
    assertEquals("mapped_value", getSingleValue(request, "new_name"));
    assertEquals("original_value", getSingleValue(request, "keep_as_is"));
  }

  @Test
  public void testFieldDropVariousPrefixes() throws UnrecoverableConversionException {
    // Test multiple drop prefixes and edge cases
    Map<String, Object> config = new HashMap<>();
    config.put("database.name", "test_db");
    config.put("table.name", "test_table");
    config.put("warehouse.path", "file:///tmp/test");
    config.put("target.index.name", INDEX_NAME);
    config.put("field.drop.prefixes", java.util.Arrays.asList("_", "sys_", "tmp"));

    PaimonConfig configMultiDrop = new PaimonConfig(config);
    PaimonToAddDocumentConverter converterMultiDrop =
        new PaimonToAddDocumentConverter(configMultiDrop);

    // Create schema with various prefixes
    RowType rowType =
        RowType.of(
            new DataField(0, "normal_field", new VarCharType(50)), // Keep
            new DataField(1, "_private", new VarCharType(50)), // Drop (starts with "_")
            new DataField(2, "sys_config", new VarCharType(50)), // Drop (starts with "sys_")
            new DataField(3, "tmpdata", new VarCharType(50)), // Drop (starts with "tmp")
            new DataField(
                4,
                "temp_file",
                new VarCharType(50)), // Keep (doesn't start with "tmp", starts with "temp")
            new DataField(
                5,
                "system",
                new VarCharType(50)) // Keep (doesn't start with "sys_", starts with "system")
            );
    converterMultiDrop.setRowType(rowType);

    // Create row with data
    GenericRow row = new GenericRow(6);
    row.setField(0, BinaryString.fromString("normal"));
    row.setField(1, BinaryString.fromString("private"));
    row.setField(2, BinaryString.fromString("config"));
    row.setField(3, BinaryString.fromString("data"));
    row.setField(4, BinaryString.fromString("temp"));
    row.setField(5, BinaryString.fromString("sys"));

    // Convert row to document
    AddDocumentRequest request = converterMultiDrop.convertRowToDocument(row);

    // Verify correct fields are kept and dropped
    assertTrue("Should contain normal_field", request.getFieldsMap().containsKey("normal_field"));
    assertTrue("Should contain temp_file", request.getFieldsMap().containsKey("temp_file"));
    assertTrue("Should contain system", request.getFieldsMap().containsKey("system"));

    assertFalse("Should NOT contain _private", request.getFieldsMap().containsKey("_private"));
    assertFalse("Should NOT contain sys_config", request.getFieldsMap().containsKey("sys_config"));
    assertFalse("Should NOT contain tmpdata", request.getFieldsMap().containsKey("tmpdata"));

    // Verify we have exactly 3 fields (3 dropped)
    assertEquals("Should have 3 fields after dropping 3", 3, request.getFieldsMap().size());

    // Verify field values
    assertEquals("normal", getSingleValue(request, "normal_field"));
    assertEquals("temp", getSingleValue(request, "temp_file"));
    assertEquals("sys", getSingleValue(request, "system"));
  }

  // Helper method to get single value from MultiValuedField
  private String getSingleValue(AddDocumentRequest request, String fieldName) {
    MultiValuedField mvf = request.getFieldsMap().get(fieldName);
    assertNotNull("Field not found: " + fieldName, mvf);
    assertEquals("Expected single value for field: " + fieldName, 1, mvf.getValueCount());
    return mvf.getValue(0);
  }
}
