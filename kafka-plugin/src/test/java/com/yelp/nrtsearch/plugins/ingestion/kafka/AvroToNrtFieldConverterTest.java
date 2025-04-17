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
package com.yelp.nrtsearch.plugins.ingestion.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class AvroToNrtFieldConverterTest {

  private AvroToNrtFieldConverter converter;

  @Before
  public void setUp() {
    converter = new AvroToNrtFieldConverter();
  }

  @Test
  public void testConvertRecordSchema() {
    // Create a simple Avro record schema
    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("stringField")
            .type()
            .stringType()
            .noDefault()
            .name("intField")
            .type()
            .intType()
            .noDefault()
            .name("booleanField")
            .type()
            .booleanType()
            .noDefault()
            .endRecord();

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify results
    assertEquals(3, fields.size());

    // Check the string field
    Field stringField = findFieldByName(fields, "stringField");
    assertEquals(FieldType.TEXT, stringField.getType());
    assertTrue(stringField.getSearch());
    assertTrue(stringField.getStore());

    // Check the int field
    Field intField = findFieldByName(fields, "intField");
    assertEquals(FieldType.INT, intField.getType());
    assertTrue(intField.getStoreDocValues());

    // Check the boolean field
    Field booleanField = findFieldByName(fields, "booleanField");
    assertEquals(FieldType.BOOLEAN, booleanField.getType());
    assertTrue(booleanField.getStoreDocValues());
  }

  @Test
  public void testConvertArraySchema() {
    // Create a record with an array field
    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("stringArrayField")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .endRecord();

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify results
    assertEquals(1, fields.size());

    // Check the array field
    Field arrayField = fields.get(0);
    assertEquals("stringArrayField", arrayField.getName());
    assertEquals(FieldType.TEXT, arrayField.getType());
    assertTrue(arrayField.getMultiValued());
  }

  @Test
  public void testConvertMapSchema() {
    // Create a record with a map field
    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("stringMapField")
            .type()
            .map()
            .values()
            .stringType()
            .noDefault()
            .endRecord();

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify results
    assertEquals(1, fields.size());

    // Check the map field
    Field mapField = fields.get(0);
    assertEquals("stringMapField", mapField.getName());
    assertEquals(FieldType.OBJECT, mapField.getType());
    // NrtSearch Field doesn't have a dynamic field setter, so we can't verify that property
  }

  @Test
  public void testConvertNestedRecordSchema() {
    // Create a nested record schema
    Schema nestedSchema =
        SchemaBuilder.record("NestedRecord")
            .fields()
            .name("nestedField1")
            .type()
            .stringType()
            .noDefault()
            .name("nestedField2")
            .type()
            .intType()
            .noDefault()
            .endRecord();

    Schema schema =
        SchemaBuilder.record("ParentRecord")
            .fields()
            .name("parentField")
            .type()
            .stringType()
            .noDefault()
            .name("nestedRecord")
            .type(nestedSchema)
            .noDefault()
            .endRecord();

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify results
    assertEquals(2, fields.size());

    // Check the nested record field
    Field nestedRecordField = findFieldByName(fields, "nestedRecord");
    assertEquals(FieldType.OBJECT, nestedRecordField.getType());
    assertEquals(2, nestedRecordField.getChildFieldsCount());

    // Check nested field 1
    Field nestedField1 = findFieldByName(nestedRecordField.getChildFieldsList(), "nestedField1");
    assertEquals(FieldType.TEXT, nestedField1.getType());

    // Check nested field 2
    Field nestedField2 = findFieldByName(nestedRecordField.getChildFieldsList(), "nestedField2");
    assertEquals(FieldType.INT, nestedField2.getType());
  }

  @Test
  public void testConvertUnionSchema() {
    // Create a record with a nullable string field (union with null)
    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("nullableStringField")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .endUnion()
            .noDefault()
            .endRecord();

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify results
    assertEquals(1, fields.size());

    // Check the nullable field
    Field nullableField = fields.get(0);
    assertEquals("nullableStringField", nullableField.getName());
    assertEquals(FieldType.TEXT, nullableField.getType());
    // Note: NrtSearch Field doesn't have an optional property in the protobuf definition
  }

  @Test
  public void testNonRecordSchemaHandling() {
    // Create a non-record schema (just a string schema)
    Schema schema = Schema.create(Schema.Type.STRING);

    // Convert the schema
    List<Field> fields = converter.convertToNrtFields(schema);

    // Verify that an empty list is returned for non-record schemas
    assertEquals(0, fields.size());
  }

  // Helper method to find a field by name in a list of fields
  private Field findFieldByName(List<Field> fields, String name) {
    return fields.stream()
        .filter(field -> field.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Field not found: " + name));
  }
}
