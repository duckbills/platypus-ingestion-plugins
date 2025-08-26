package com.yelp.nrtsearch.plugins.ingestion.kafka;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/** Converts an Avro GenericRecord into an AddDocumentRequest for NrtSearch. */
public class AvroToAddDocumentConverter {
  /**
   * Convert a GenericRecord to an AddDocumentRequest.
   *
   * @param record the Avro GenericRecord
   * @param indexName the NrtSearch index name
   * @return AddDocumentRequest ready to be sent to NrtSearch
   */
  public AddDocumentRequest convertToNrtDocument(GenericRecord record, String indexName) {
    AddDocumentRequest.Builder requestBuilder =
        AddDocumentRequest.newBuilder().setIndexName(indexName);
    Map<String, MultiValuedField> fieldMap = new LinkedHashMap<>();
    processRecord(record, "", fieldMap);

    requestBuilder.putAllFields(fieldMap);
    return requestBuilder.build();
  }

  /**
   * Recursively process a GenericRecord and populate the field map.
   *
   * @param record the Avro record
   * @param prefix dot notation prefix for nested fields
   * @param fieldMap the map to populate with field values
   */
  private void processRecord(
      GenericRecord record, String prefix, Map<String, MultiValuedField> fieldMap) {
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = prefix.isEmpty() ? field.name() : prefix + "_" + field.name();
      Object value = record.get(field.name());
      if (value == null) {
        continue;
      }
      Schema fieldSchema = getNonNullSchema(field.schema());
      switch (fieldSchema.getType()) {
        case RECORD:
          processRecord((GenericRecord) value, fieldName, fieldMap);
          break;
        case ARRAY:
          List values = new ArrayList<>();
          Iterable iterable =
              (value instanceof Collection) ? (Collection) value : (Iterable<?>) value;
          for (Object item : iterable) {
            if (item != null) {
              values.add(convertValueToString(item));
            }
          }
          if (!values.isEmpty()) {
            fieldMap.put(fieldName, MultiValuedField.newBuilder().addAllValue(values).build());
          }
          break;
        default:
          fieldMap.put(
              fieldName,
              MultiValuedField.newBuilder().addValue(convertValueToString(value)).build());
          break;
      }
    }
  }

  /**
   * Extract the non-null schema from a UNION type.
   *
   * @param schema the Avro schema
   * @return the non-null schema if UNION, otherwise the original schema
   */
  private Schema getNonNullSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema s : schema.getTypes()) {
        if (s.getType() != Schema.Type.NULL) {
          return s;
        }
      }
    }
    return schema;
  }

  /**
   * Convert an Avro value to its string representation.
   *
   * @param value the Avro value
   * @return string representation
   */
  private String convertValueToString(Object value) {
    if (value instanceof Utf8) {
      return value.toString(); // Utf8 to String
    } else if (value instanceof CharSequence) {
      return value.toString();
    } else if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    } else if (value instanceof GenericRecord) {
      // Should not happen here; RECORDs are handled recursively
      return value.toString();
    } else {
      return value.toString(); // fallback
    }
  }
}
