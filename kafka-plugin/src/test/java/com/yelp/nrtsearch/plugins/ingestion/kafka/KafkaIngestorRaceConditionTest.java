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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for race condition handling in KafkaIngestor when plugin starts before index is
 * created/started.
 */
public class KafkaIngestorRaceConditionTest {

  private KafkaIngestor ingestor;
  private NrtsearchConfig config;

  @Before
  public void setUp() throws Exception {
    // Create test configuration
    String configYaml =
        """
            nodeName: "test-server"
            pluginConfigs:
              ingestion:
                kafka:
                  bootstrapServers: "localhost:9092"
                  topic: "test-topic"
                  groupId: "test-group"
                  indexName: "test-index"
                  schemaRegistryUrl: "http://localhost:8081"
                  autoOffsetReset: "earliest"
                  batchSize: "10"
            """;

    config = new NrtsearchConfig(new ByteArrayInputStream(configYaml.getBytes()));
    // Use fast retry for tests: 3 retries with 10ms delay instead of 5 seconds
    ingestor = new KafkaIngestor(config, Executors.newSingleThreadExecutor(), 1, 3, 10L);
  }

  @Test
  public void testRetryOnIndexNotExist() throws Exception {
    // Create a partial mock of the ingestor to control addDocuments behavior
    KafkaIngestor spyIngestor = spy(ingestor);

    // Mock the addDocuments method to simulate "index does not exist" error initially,
    // then succeed on the second attempt
    StatusRuntimeException indexNotExistException =
        Status.INVALID_ARGUMENT
            .withDescription("Index test-index does not exist, unable to add documents")
            .asRuntimeException();

    doThrow(indexNotExistException)
        .doReturn(1L) // Second call succeeds, return sequence number
        .when(spyIngestor)
        .addDocuments(any(), anyString());

    // Mock commit to do nothing (successful)
    doNothing().when(spyIngestor).commit(anyString());

    // Create test documents
    AddDocumentRequest testDoc =
        AddDocumentRequest.newBuilder()
            .setIndexName("test-index")
            .putFields(
                "id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("test-doc").build())
            .build();

    List<AddDocumentRequest> docs = List.of(testDoc);

    // Use reflection to access the private indexAndCommitBatch method
    Method indexAndCommitBatchMethod =
        KafkaIngestor.class.getDeclaredMethod("indexAndCommitBatch", List.class);
    indexAndCommitBatchMethod.setAccessible(true);

    // This should succeed after retrying (first call fails, second succeeds)
    indexAndCommitBatchMethod.invoke(spyIngestor, docs);

    // Verify that addDocuments was called twice (retry logic worked)
    verify(spyIngestor, times(2)).addDocuments(docs, "test-index");
    verify(spyIngestor, times(1)).commit("test-index"); // Only called once on success
  }

  @Test
  public void testFailAfterMaxRetries() throws Exception {
    // Create a partial mock of the ingestor
    KafkaIngestor spyIngestor = spy(ingestor);

    // Mock the addDocuments method to always throw "index does not exist" error
    StatusRuntimeException indexNotExistException =
        Status.INVALID_ARGUMENT
            .withDescription("Index test-index does not exist, unable to add documents")
            .asRuntimeException();

    doThrow(indexNotExistException).when(spyIngestor).addDocuments(any(), anyString());

    // Create test documents
    AddDocumentRequest testDoc =
        AddDocumentRequest.newBuilder()
            .setIndexName("test-index")
            .putFields(
                "id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("test-doc").build())
            .build();

    List<AddDocumentRequest> docs = List.of(testDoc);

    // Use reflection to access the private indexAndCommitBatch method
    Method indexAndCommitBatchMethod =
        KafkaIngestor.class.getDeclaredMethod("indexAndCommitBatch", List.class);
    indexAndCommitBatchMethod.setAccessible(true);

    // This should fail after max retries
    assertThatThrownBy(() -> indexAndCommitBatchMethod.invoke(spyIngestor, docs))
        .hasCauseInstanceOf(StatusRuntimeException.class)
        .getCause()
        .hasMessageContaining("does not exist, unable to add documents");

    // Verify that addDocuments was called 3 times (max retries)
    verify(spyIngestor, times(3)).addDocuments(docs, "test-index");
    verify(spyIngestor, never()).commit(anyString()); // Never successful, so commit never called
  }

  @Test
  public void testNoRetryOnDifferentError() throws Exception {
    // Create a partial mock of the ingestor
    KafkaIngestor spyIngestor = spy(ingestor);

    // Mock the addDocuments method to throw a different error (not index missing)
    StatusRuntimeException differentException =
        Status.INTERNAL.withDescription("Some other error").asRuntimeException();

    doThrow(differentException).when(spyIngestor).addDocuments(any(), anyString());

    // Create test documents
    AddDocumentRequest testDoc =
        AddDocumentRequest.newBuilder()
            .setIndexName("test-index")
            .putFields(
                "id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("test-doc").build())
            .build();

    List<AddDocumentRequest> docs = List.of(testDoc);

    // Use reflection to access the private indexAndCommitBatch method
    Method indexAndCommitBatchMethod =
        KafkaIngestor.class.getDeclaredMethod("indexAndCommitBatch", List.class);
    indexAndCommitBatchMethod.setAccessible(true);

    // This should fail immediately (no retries for different errors)
    assertThatThrownBy(() -> indexAndCommitBatchMethod.invoke(spyIngestor, docs))
        .hasCauseInstanceOf(StatusRuntimeException.class)
        .getCause()
        .hasMessageContaining("Some other error");

    // Verify that addDocuments was called only once (no retry for different errors)
    verify(spyIngestor, times(1)).addDocuments(docs, "test-index");
    verify(spyIngestor, never()).commit(anyString()); // Never successful, so commit never called
  }

  @Test
  public void testRetryWithSlowSetup() throws Exception {
    // Create a partial mock of the ingestor
    KafkaIngestor spyIngestor = spy(ingestor);

    // Mock a realistic scenario: first 2 calls fail (index not ready), 3rd succeeds
    StatusRuntimeException indexNotExistException =
        Status.INVALID_ARGUMENT
            .withDescription("Index test-index does not exist, unable to add documents")
            .asRuntimeException();

    doThrow(indexNotExistException)
        .doThrow(indexNotExistException)
        .doReturn(1L) // Third call succeeds, return sequence number
        .when(spyIngestor)
        .addDocuments(any(), anyString());

    doNothing().when(spyIngestor).commit(anyString());

    // Create test documents
    AddDocumentRequest testDoc =
        AddDocumentRequest.newBuilder()
            .setIndexName("test-index")
            .putFields(
                "id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("test-doc").build())
            .build();

    List<AddDocumentRequest> docs = List.of(testDoc);

    // Use reflection to access the private indexAndCommitBatch method
    Method indexAndCommitBatchMethod =
        KafkaIngestor.class.getDeclaredMethod("indexAndCommitBatch", List.class);
    indexAndCommitBatchMethod.setAccessible(true);

    // This should succeed on the third attempt (will take ~20ms due to fast retry)
    long startTime = System.currentTimeMillis();
    indexAndCommitBatchMethod.invoke(spyIngestor, docs);
    long duration = System.currentTimeMillis() - startTime;

    // Should have taken at least 20ms (2 retries * 10ms each) but less than 1 second
    assertThat(duration).isGreaterThan(20L).isLessThan(1000L);

    // Verify that addDocuments was called 3 times (2 failures + 1 success)
    verify(spyIngestor, times(3)).addDocuments(docs, "test-index");
    verify(spyIngestor, times(1)).commit("test-index"); // Called once on success
  }
}
