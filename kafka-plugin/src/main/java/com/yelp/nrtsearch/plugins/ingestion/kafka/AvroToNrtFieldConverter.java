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

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts Avro schemas to NrtSearch field definitions. */
public class AvroToNrtFieldConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroToNrtFieldConverter.class);

  /**
   * Convert an Avro schema to NrtSearch Field objects.
   *
   * @param schema The Avro schema to convert
   * @return A list of Field objects
   */
  public List<Field> convertToNrtFields(Schema schema) {
    List<Field> fields = new ArrayList<>();

    if (schema.getType() == Schema.Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        fields.add(convertField(field.name(), field.schema()));
      }
    } else {
      LOGGER.warn("Expected RECORD schema but got {}", schema.getType());
    }

    return fields;
  }

  /**
   * Convert a single Avro field to a NrtSearch Field object.
   *
   * @param fieldName The name of the field
   * @param fieldSchema The Avro schema for the field
   * @return A NrtSearch Field object
   */
  private Field convertField(String fieldName, Schema fieldSchema) {
    Field.Builder fieldBuilder = Field.newBuilder().setName(fieldName);

    // Handle UNION types (often used for nullable fields)
    if (fieldSchema.getType() == Schema.Type.UNION) {
      List<Schema> types = fieldSchema.getTypes();
      Schema nonNullSchema = null;

      // Find the non-null type in the union
      for (Schema unionType : types) {
        if (unionType.getType() != Schema.Type.NULL) {
          nonNullSchema = unionType;
          break;
        }
      }

      if (nonNullSchema != null) {
        setFieldType(fieldBuilder, nonNullSchema);
      } else {
        // If all types are NULL, default to STRING
        fieldBuilder.setType(FieldType.TEXT);
      }

      // If NULL is one of the types, the field is optional
      // Note: Field doesn't have setOptional() in the protobuf definition
      // so we need to handle this differently in the final schema
    } else {
      setFieldType(fieldBuilder, fieldSchema);
    }

    // Handle arrays (set multiValued=true for NrtSearch)
    if (fieldSchema.getType() == Schema.Type.ARRAY) {
      fieldBuilder.setMultiValued(true);
    }

    return fieldBuilder.build();
  }

  /**
   * Set the appropriate NrtSearch field type based on the Avro schema type.
   *
   * @param fieldBuilder The Field.Builder to configure
   * @param schema The Avro schema to convert
   */
  private void setFieldType(Field.Builder fieldBuilder, Schema schema) {
    switch (schema.getType()) {
      case STRING:
        fieldBuilder.setType(FieldType.TEXT);
        fieldBuilder.setSearch(true);
        fieldBuilder.setStore(true);
        break;

      case ENUM:
        fieldBuilder.setType(FieldType.ATOM);
        fieldBuilder.setStore(true);
        break;

      case INT:
        fieldBuilder.setType(FieldType.INT);
        fieldBuilder.setStoreDocValues(true);
        break;

      case LONG:
        fieldBuilder.setType(FieldType.LONG);
        fieldBuilder.setStoreDocValues(true);
        break;

      case FLOAT:
        fieldBuilder.setType(FieldType.FLOAT);
        fieldBuilder.setStoreDocValues(true);
        break;

      case DOUBLE:
        fieldBuilder.setType(FieldType.DOUBLE);
        fieldBuilder.setStoreDocValues(true);
        break;

      case BOOLEAN:
        fieldBuilder.setType(FieldType.BOOLEAN);
        fieldBuilder.setStoreDocValues(true);
        break;

      case RECORD:
        fieldBuilder.setType(FieldType.OBJECT);

        List<Field> subFields = new ArrayList<>();
        for (Schema.Field subField : schema.getFields()) {
          subFields.add(convertField(subField.name(), subField.schema()));
        }

        // Field doesn't have addAllSubFields in the protobuf, it uses childFields
        for (Field subField : subFields) {
          fieldBuilder.addChildFields(subField);
        }
        break;

      case ARRAY:
        // Handle array element type
        setFieldType(fieldBuilder, schema.getElementType());
        fieldBuilder.setMultiValued(true);
        break;

      case MAP:
        // Maps are a bit tricky, using OBJECT type with dynamic fields
        // Note: Field doesn't have setDynamic() in the protobuf definition
        fieldBuilder.setType(FieldType.OBJECT);
        // We'll mark it as an object but can't set 'dynamic' flag
        break;

      case BYTES:
      case FIXED:
        // BINARY is not in the FieldType enum, use TEXT instead
        fieldBuilder.setType(FieldType.TEXT);
        fieldBuilder.setStore(true);
        break;

      default:
        // Default to TEXT for unknown types
        LOGGER.warn("Unsupported Avro type: {}, defaulting to TEXT", schema.getType());
        fieldBuilder.setType(FieldType.TEXT);
        fieldBuilder.setStore(true);
        break;
    }
  }
}
