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

  // Helper method to get single value from MultiValuedField
  private String getSingleValue(AddDocumentRequest request, String fieldName) {
    MultiValuedField mvf = request.getFieldsMap().get(fieldName);
    assertNotNull("Field not found: " + fieldName, mvf);
    assertEquals("Expected single value for field: " + fieldName, 1, mvf.getValueCount());
    return mvf.getValue(0);
  }
}
