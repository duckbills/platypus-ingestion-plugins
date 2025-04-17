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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldType;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaConversionServiceTest {

  @Mock private SchemaRegistryClient mockSchemaRegistryClient;

  private SchemaConversionService service;

  // Sample Avro schemas for testing
  private static final String SIMPLE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"SimpleRecord\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"value\",\"type\":\"int\"},"
          + "{\"name\":\"description\",\"type\":[\"null\",\"string\"]}"
          + "]}";

  private static final String COMPLEX_SCHEMA =
      "{\"type\":\"record\",\"name\":\"ComplexRecord\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"metrics\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},"
          + "{\"name\":\"metadata\",\"type\":{\"type\":\"record\",\"name\":\"Metadata\",\"fields\":["
          + "  {\"name\":\"created\",\"type\":\"long\"},"
          + "  {\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
          + "]}},"
          + "{\"name\":\"active\",\"type\":\"boolean\"}"
          + "]}";

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    service =
        new SchemaConversionService("http://mock-registry:8081") {
          @Override
          protected SchemaRegistryClient createSchemaRegistryClient(String url, int cacheSize) {
            return mockSchemaRegistryClient;
          }
        };
  }

  @Test
  public void testGetFieldsForTopic_SimpleSchema() throws IOException, RestClientException {
    // Arrange
    String topic = "test-topic";
    String subject = topic + "-value";

    SchemaMetadata mockMetadata = mock(SchemaMetadata.class);
    when(mockMetadata.getSchema()).thenReturn(SIMPLE_SCHEMA);
    when(mockSchemaRegistryClient.getLatestSchemaMetadata(subject)).thenReturn(mockMetadata);

    // Act
    List<Field> fields = service.getFieldsForTopic(topic, false);

    // Assert
    assertNotNull(fields);
    assertEquals(3, fields.size());

    // Check id field
    Field idField = findFieldByName(fields, "id");
    assertEquals(FieldType.TEXT, idField.getType());
    assertEquals(true, idField.getSearch());
    assertEquals(true, idField.getStore());

    // Check value field
    Field valueField = findFieldByName(fields, "value");
    assertEquals(FieldType.INT, valueField.getType());
    assertEquals(true, valueField.getStoreDocValues());

    // Check description field (nullable)
    Field descField = findFieldByName(fields, "description");
    assertEquals(FieldType.TEXT, descField.getType());
  }

  @Test
  public void testGetFieldsForTopic_ComplexSchema() throws IOException, RestClientException {
    // Arrange
    String topic = "complex-topic";
    String subject = topic + "-value";

    SchemaMetadata mockMetadata = mock(SchemaMetadata.class);
    when(mockMetadata.getSchema()).thenReturn(COMPLEX_SCHEMA);
    when(mockSchemaRegistryClient.getLatestSchemaMetadata(subject)).thenReturn(mockMetadata);

    // Act
    List<Field> fields = service.getFieldsForTopic(topic, false);

    // Assert
    assertNotNull(fields);
    assertEquals(4, fields.size());

    // Check id field
    Field idField = findFieldByName(fields, "id");
    assertEquals(FieldType.TEXT, idField.getType());

    // Check metrics field (array of doubles)
    Field metricsField = findFieldByName(fields, "metrics");
    assertEquals(FieldType.DOUBLE, metricsField.getType());
    assertEquals(true, metricsField.getMultiValued());

    // Check metadata field (nested record)
    Field metadataField = findFieldByName(fields, "metadata");
    assertEquals(FieldType.OBJECT, metadataField.getType());
    assertEquals(2, metadataField.getChildFieldsCount());

    // Check nested fields
    Field createdField = findFieldByName(metadataField.getChildFieldsList(), "created");
    assertEquals(FieldType.LONG, createdField.getType());

    Field tagsField = findFieldByName(metadataField.getChildFieldsList(), "tags");
    assertEquals(FieldType.TEXT, tagsField.getType());
    assertEquals(true, tagsField.getMultiValued());

    // Check active field
    Field activeField = findFieldByName(fields, "active");
    assertEquals(FieldType.BOOLEAN, activeField.getType());
  }

  @Test
  public void testGenerateFieldDefRequest() throws IOException, RestClientException {
    // Arrange
    String topic = "test-topic";
    String indexName = "test-index";
    String subject = topic + "-value";

    SchemaMetadata mockMetadata = mock(SchemaMetadata.class);
    when(mockMetadata.getSchema()).thenReturn(SIMPLE_SCHEMA);
    when(mockSchemaRegistryClient.getLatestSchemaMetadata(subject)).thenReturn(mockMetadata);

    // Act
    FieldDefRequest request = service.generateFieldDefRequest(indexName, topic, false);

    // Assert
    assertNotNull(request);
    assertEquals(indexName, request.getIndexName());
    assertEquals(3, request.getFieldCount());
  }

  @Test(expected = IOException.class)
  public void testSchemaRegistryError() throws IOException, RestClientException {
    // Arrange
    String topic = "test-topic";
    String subject = topic + "-value";

    when(mockSchemaRegistryClient.getLatestSchemaMetadata(subject))
        .thenThrow(new RestClientException("Schema not found", 404, 40401));

    // Act - should throw IOException
    service.getFieldsForTopic(topic, false);
  }

  @Test
  public void testKeySchema() throws IOException, RestClientException {
    // Arrange
    String topic = "test-topic";
    String subject = topic + "-key"; // Key schema has -key suffix

    SchemaMetadata mockMetadata = mock(SchemaMetadata.class);
    when(mockMetadata.getSchema()).thenReturn(SIMPLE_SCHEMA);
    when(mockSchemaRegistryClient.getLatestSchemaMetadata(subject)).thenReturn(mockMetadata);

    // Act
    List<Field> fields = service.getFieldsForTopic(topic, true);

    // Assert
    assertNotNull(fields);
    assertEquals(3, fields.size());
  }

  // Helper method to find a field by name in a list of fields
  private Field findFieldByName(List<Field> fields, String name) {
    return fields.stream()
        .filter(field -> field.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Field not found: " + name));
  }
}
