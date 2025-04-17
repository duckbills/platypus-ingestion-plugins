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
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for fetching Avro schemas from Confluent Schema Registry and converting them
 * to NrtSearch field definitions.
 */
public class SchemaConversionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConversionService.class);

  private final SchemaRegistryClient schemaRegistryClient;
  private final AvroToNrtFieldConverter converter;

  /**
   * Create a new SchemaConversionService with the specified Schema Registry URL.
   *
   * @param schemaRegistryUrl URL of the Confluent Schema Registry
   */
  public SchemaConversionService(String schemaRegistryUrl) {
    this.schemaRegistryClient = createSchemaRegistryClient(schemaRegistryUrl, 100);
    this.converter = new AvroToNrtFieldConverter();
  }

  /**
   * Create a Schema Registry client. Protected to allow overriding in tests.
   *
   * @param url URL of the Schema Registry
   * @param cacheSize Size of the schema cache
   * @return A SchemaRegistryClient instance
   */
  protected SchemaRegistryClient createSchemaRegistryClient(String url, int cacheSize) {
    return new CachedSchemaRegistryClient(url, cacheSize);
  }

  /**
   * Fetch the latest schema for a topic and convert it to NrtSearch field definitions.
   *
   * @param topic The Kafka topic name (assumed to be same as subject name in Schema Registry)
   * @param isKey Whether to fetch the key schema (true) or value schema (false)
   * @return A list of Field objects for NrtSearch
   * @throws IOException If there's an error communicating with the Schema Registry
   */
  public List<Field> getFieldsForTopic(String topic, boolean isKey) throws IOException {
    String subject = topic + (isKey ? "-key" : "-value");
    try {
      SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      Schema avroSchema = new Schema.Parser().parse(metadata.getSchema());
      LOGGER.info("Retrieved schema for subject {}: {}", subject, avroSchema.getName());

      return converter.convertToNrtFields(avroSchema);
    } catch (RestClientException e) {
      LOGGER.error("Error fetching schema for subject {}", subject, e);
      throw new IOException("Failed to fetch schema from Schema Registry", e);
    }
  }

  /**
   * Generate a FieldDefRequest for the specified topic and index.
   *
   * @param indexName The NrtSearch index name
   * @param topic The Kafka topic name
   * @param isKey Whether to use the key schema (true) or value schema (false)
   * @return A FieldDefRequest object ready to be used with NrtSearch's internal APIs
   * @throws IOException If there's an error communicating with the Schema Registry
   */
  public FieldDefRequest generateFieldDefRequest(String indexName, String topic, boolean isKey)
      throws IOException {
    List<Field> fields = getFieldsForTopic(topic, isKey);

    return FieldDefRequest.newBuilder().setIndexName(indexName).addAllField(fields).build();
  }
}
