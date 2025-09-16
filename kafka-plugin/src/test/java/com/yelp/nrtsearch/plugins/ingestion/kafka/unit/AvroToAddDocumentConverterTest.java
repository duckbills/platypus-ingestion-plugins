package com.yelp.nrtsearch.plugins.ingestion.kafka.unit;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.plugins.ingestion.kafka.AvroToAddDocumentConverter;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class AvroToAddDocumentConverterTest {

  private final AvroToAddDocumentConverter converter = new AvroToAddDocumentConverter();

  private static final String INDEX_NAME = "test_index";

  @Test
  public void testPrimitiveFields() {
    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("stringField")
            .requiredInt("intField")
            .requiredLong("longField")
            .requiredFloat("floatField")
            .requiredDouble("doubleField")
            .requiredBoolean("booleanField")
            .endRecord();

    GenericRecord record = new GenericData.Record(schema);
    record.put("stringField", "hello");
    record.put("intField", 42);
    record.put("longField", 123456789L);
    record.put("floatField", 3.14f);
    record.put("doubleField", 2.718);
    record.put("booleanField", true);

    AddDocumentRequest request = converter.convertToNrtDocument(record, INDEX_NAME);

    assertEquals(INDEX_NAME, request.getIndexName());
    assertEquals("hello", getSingleValue(request, "stringField"));
    assertEquals("42", getSingleValue(request, "intField"));
    assertEquals("123456789", getSingleValue(request, "longField"));
    assertEquals("3.14", getSingleValue(request, "floatField"));
    assertEquals("2.718", getSingleValue(request, "doubleField"));
    assertEquals("true", getSingleValue(request, "booleanField"));
  }

  @Test
  public void testLogicalTypes() {
    Schema schema =
        SchemaBuilder.record("LogicalRecord")
            .fields()
            .name("dateField")
            .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
            .noDefault()
            .name("timestampField")
            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    GenericRecord record = new GenericData.Record(schema);
    record.put("dateField", (int) LocalDate.of(2024, 1, 1).toEpochDay());
    record.put("timestampField", Instant.parse("2024-01-01T12:00:00Z").toEpochMilli());

    AddDocumentRequest request = converter.convertToNrtDocument(record, INDEX_NAME);

    assertEquals(
        "2024-01-01",
        LocalDate.ofEpochDay(Long.parseLong(getSingleValue(request, "dateField"))).toString());
    assertEquals(
        "2024-01-01T12:00:00Z",
        Instant.ofEpochMilli(Long.parseLong(getSingleValue(request, "timestampField"))).toString());
  }

  @Test
  public void testNullableFields() {
    Schema schema =
        SchemaBuilder.record("NullableRecord")
            .fields()
            .name("nullableString")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .endUnion()
            .nullDefault()
            .name("nullableInt")
            .type()
            .unionOf()
            .nullType()
            .and()
            .intType()
            .endUnion()
            .nullDefault()
            .endRecord();

    GenericRecord record = new GenericData.Record(schema);
    record.put("nullableString", "nullable");
    record.put("nullableInt", 99);

    AddDocumentRequest request = converter.convertToNrtDocument(record, INDEX_NAME);

    assertEquals("nullable", getSingleValue(request, "nullableString"));
    assertEquals("99", getSingleValue(request, "nullableInt"));
  }

  @Test
  public void testArrayField() {
    Schema schema =
        SchemaBuilder.record("ArrayRecord")
            .fields()
            .name("tags")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .endRecord();

    GenericRecord record = new GenericData.Record(schema);

    List tags = Arrays.asList("tag1", "tag2", "tag3");
    Schema arraySchema = schema.getField("tags").schema();
    GenericArray avroArray = new GenericData.Array<>(arraySchema, tags);

    record.put("tags", avroArray);

    AddDocumentRequest request = converter.convertToNrtDocument(record, INDEX_NAME);

    assertEquals(tags, request.getFieldsMap().get("tags").getValueList());
  }

  @Test
  public void testNestedRecord() {
    Schema nestedSchema =
        SchemaBuilder.record("Nested").fields().requiredString("innerField").endRecord();

    Schema schema =
        SchemaBuilder.record("Parent")
            .fields()
            .name("nested")
            .type(nestedSchema)
            .noDefault()
            .endRecord();

    GenericRecord nested = new GenericData.Record(nestedSchema);
    nested.put("innerField", "value");

    GenericRecord parent = new GenericData.Record(schema);
    parent.put("nested", nested);

    AddDocumentRequest request = converter.convertToNrtDocument(parent, INDEX_NAME);
    assertEquals("value", getSingleValue(request, "nested_innerField"));
  }

  @Test
  public void testArrayOfRecords() {
    Schema childSchema = SchemaBuilder.record("Child").fields().requiredString("name").endRecord();

    Schema schema =
        SchemaBuilder.record("Parent")
            .fields()
            .name("children")
            .type()
            .array()
            .items(childSchema)
            .noDefault()
            .endRecord();

    GenericRecord child1 = new GenericData.Record(childSchema);
    child1.put("name", "Alice");

    GenericRecord child2 = new GenericData.Record(childSchema);
    child2.put("name", "Bob");

    GenericRecord parent = new GenericData.Record(schema);
    parent.put("children", Arrays.asList(child1, child2));

    AddDocumentRequest request = converter.convertToNrtDocument(parent, INDEX_NAME);

    // Since nested records in arrays are not flattened, they are stringified
    List<String> values = request.getFieldsMap().get("children").getValueList();
    assertTrue(values.get(0).contains("Alice"));
    assertTrue(values.get(1).contains("Bob"));
  }

  @Test
  public void testUtf8String() {
    Schema schema =
        SchemaBuilder.record("Utf8Record").fields().requiredString("utf8Field").endRecord();

    GenericRecord record = new GenericData.Record(schema);
    record.put("utf8Field", new Utf8("utf8-value"));

    AddDocumentRequest request = converter.convertToNrtDocument(record, INDEX_NAME);
    assertEquals("utf8-value", getSingleValue(request, "utf8Field"));
  }

  // Helper to get single value from MultiValuedField
  private String getSingleValue(AddDocumentRequest request, String fieldName) {
    MultiValuedField mvf = request.getFieldsMap().get(fieldName);
    assertNotNull("Field not found: " + fieldName, mvf);
    assertEquals("Expected single value for field: " + fieldName, 1, mvf.getValueCount());
    return mvf.getValue(0);
  }
}
