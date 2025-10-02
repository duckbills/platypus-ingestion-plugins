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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.Before;
import org.junit.Test;

public class PaimonRowProcessorTest {

  private TestIngestor testIngestor;
  private PaimonRowProcessor processor;
  private TestConverter converter;

  @Before
  public void setUp() {
    testIngestor = new TestIngestor();
    converter = new TestConverter();

    // Create RowType with single BIGINT field at index 0 for photo_id
    RowType rowType = new RowType(List.of(new DataField(0, "photo_id", new BigIntType())));

    // Create initializer that returns ID field info
    PaimonRowProcessor.IdFieldInitializer initializer =
        () -> new PaimonRowProcessor.IdFieldInfo("photo_id", 0);
    processor = new PaimonRowProcessor("test_index", initializer, converter, testIngestor, rowType);
  }

  @Test
  public void testInsertOnly() throws Exception {
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.INSERT, 101L));
    processor.processRow(createRow(RowKind.INSERT, 102L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L, 101L, 102L), ops.get(0).ids);
    assertEquals(1, testIngestor.getCommitCount());
  }

  @Test
  public void testDeleteOnly() throws Exception {
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.processRow(createRow(RowKind.DELETE, 101L));
    processor.processRow(createRow(RowKind.DELETE, 102L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.DELETE, ops.get(0).type);
    assertEquals(List.of(100L, 101L, 102L), ops.get(0).ids);
  }

  @Test
  public void testUpdatePairSkipsUpdateBefore() throws Exception {
    processor.processRow(createRow(RowKind.UPDATE_BEFORE, 100L));
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L), ops.get(0).ids);
  }

  @Test
  public void testDeleteThenInsert() throws Exception {
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(2, ops.size());
    assertEquals(OperationType.DELETE, ops.get(0).type);
    assertEquals(List.of(100L), ops.get(0).ids);
    assertEquals(OperationType.ADD, ops.get(1).type);
    assertEquals(List.of(100L), ops.get(1).ids);
  }

  @Test
  public void testInsertThenDelete() throws Exception {
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(2, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L), ops.get(0).ids);
    assertEquals(OperationType.DELETE, ops.get(1).type);
    assertEquals(List.of(100L), ops.get(1).ids);
  }

  @Test
  public void testComplexOrdering() throws Exception {
    // +D, +I, -U, +U, +D
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.UPDATE_BEFORE, 100L));
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 100L));
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(3, ops.size());
    assertEquals(OperationType.DELETE, ops.get(0).type);
    assertEquals(OperationType.ADD, ops.get(1).type);
    assertEquals(OperationType.DELETE, ops.get(2).type);
  }

  @Test
  public void testBatchingConsecutiveOperations() throws Exception {
    // Multiple inserts (should batch)
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.INSERT, 101L));
    processor.processRow(createRow(RowKind.INSERT, 102L));
    // Type switch - flush
    processor.processRow(createRow(RowKind.DELETE, 200L));
    processor.processRow(createRow(RowKind.DELETE, 201L));
    // Type switch - flush
    processor.processRow(createRow(RowKind.INSERT, 300L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(3, ops.size());

    // First batch: 3 inserts
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L, 101L, 102L), ops.get(0).ids);

    // Second batch: 2 deletes
    assertEquals(OperationType.DELETE, ops.get(1).type);
    assertEquals(List.of(200L, 201L), ops.get(1).ids);

    // Third batch: 1 insert
    assertEquals(OperationType.ADD, ops.get(2).type);
    assertEquals(List.of(300L), ops.get(2).ids);
  }

  @Test
  public void testUpdateAfterWithoutUpdateBefore() throws Exception {
    // Standalone +U (no preceding -U)
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L), ops.get(0).ids);
  }

  @Test
  public void testMultipleUpdatePairs() throws Exception {
    // -U1, +U1, -U2, +U2
    processor.processRow(createRow(RowKind.UPDATE_BEFORE, 100L));
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 100L));
    processor.processRow(createRow(RowKind.UPDATE_BEFORE, 101L));
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 101L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(100L, 101L), ops.get(0).ids);
  }

  @Test
  public void testDeleteInsertDeleteSameId() throws Exception {
    // +D(100), +I(100), +D(100)
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(3, ops.size());
    assertEquals(OperationType.DELETE, ops.get(0).type);
    assertEquals(OperationType.ADD, ops.get(1).type);
    assertEquals(OperationType.DELETE, ops.get(2).type);
    // All for same ID
    assertEquals(List.of(100L), ops.get(0).ids);
    assertEquals(List.of(100L), ops.get(1).ids);
    assertEquals(List.of(100L), ops.get(2).ids);
  }

  @Test
  public void testEmptyBatch() throws Exception {
    processor.flush();
    List<Operation> ops = testIngestor.getOperations();
    assertEquals(0, ops.size());
  }

  @Test
  public void testOnlyUpdateBefore() throws Exception {
    // Standalone -U (no following +U) - should be skipped
    processor.processRow(createRow(RowKind.UPDATE_BEFORE, 100L));
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(0, ops.size());
  }

  @Test
  public void testLeftoverDeleteBatchFlushedAtEnd() throws Exception {
    // Process some deletes but don't trigger type transition
    processor.processRow(createRow(RowKind.DELETE, 100L));
    processor.processRow(createRow(RowKind.DELETE, 101L));
    processor.processRow(createRow(RowKind.DELETE, 102L));

    // No flush yet - verify nothing executed
    assertEquals(0, testIngestor.getOperations().size());

    // Now flush - should flush the delete batch
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.DELETE, ops.get(0).type);
    assertEquals(List.of(100L, 101L, 102L), ops.get(0).ids);
  }

  @Test
  public void testLeftoverAddBatchFlushedAtEnd() throws Exception {
    // Process some adds but don't trigger type transition
    processor.processRow(createRow(RowKind.INSERT, 200L));
    processor.processRow(createRow(RowKind.INSERT, 201L));
    processor.processRow(createRow(RowKind.UPDATE_AFTER, 202L));

    // No flush yet - verify nothing executed
    assertEquals(0, testIngestor.getOperations().size());

    // Now flush - should flush the add batch
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(1, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(List.of(200L, 201L, 202L), ops.get(0).ids);
  }

  @Test
  public void testMultipleFlushCallsAreIdempotent() throws Exception {
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.flush();

    // First flush should execute
    assertEquals(1, testIngestor.getOperations().size());

    // Second flush should be no-op
    processor.flush();
    assertEquals(1, testIngestor.getOperations().size());

    // Third flush should also be no-op
    processor.flush();
    assertEquals(1, testIngestor.getOperations().size());
  }

  @Test
  public void testFlushAfterTypeTransitionStillFlushesRemaining() throws Exception {
    // Process adds, switch to delete (triggers flush), add more deletes
    processor.processRow(createRow(RowKind.INSERT, 100L));
    processor.processRow(createRow(RowKind.INSERT, 101L));
    // Type switch - flushes adds
    processor.processRow(createRow(RowKind.DELETE, 200L));
    processor.processRow(createRow(RowKind.DELETE, 201L));

    // At this point: adds were flushed, deletes are batched
    assertEquals(1, testIngestor.getOperations().size());

    // Flush should flush remaining deletes
    processor.flush();

    List<Operation> ops = testIngestor.getOperations();
    assertEquals(2, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(OperationType.DELETE, ops.get(1).type);
  }

  @Test
  public void testLargeConsecutiveBatch() throws Exception {
    // Process a large number of consecutive inserts
    // With batch size of 1000, should auto-flush after exactly 1000 rows
    for (long i = 0; i < 1000; i++) {
      processor.processRow(createRow(RowKind.INSERT, i));
    }

    // Should have auto-flushed once after hitting batch size of 1000
    assertEquals(1, testIngestor.getOperations().size());

    // Process a few more to verify batching continues
    for (long i = 1000; i < 1010; i++) {
      processor.processRow(createRow(RowKind.INSERT, i));
    }

    // Should still have just 1 operation (the 10 new ones are batched)
    assertEquals(1, testIngestor.getOperations().size());

    // Explicit flush for remaining
    processor.flush();

    // Now should have 2 operations: first 1000, then remaining 10
    List<Operation> ops = testIngestor.getOperations();
    assertEquals(2, ops.size());
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(1000, ops.get(0).ids.size());
    assertEquals(OperationType.ADD, ops.get(1).type);
    assertEquals(10, ops.get(1).ids.size());

    // Verify first batch IDs
    assertEquals(Long.valueOf(0L), ops.get(0).ids.get(0));
    assertEquals(Long.valueOf(999L), ops.get(0).ids.get(999));
    // Verify second batch IDs
    assertEquals(Long.valueOf(1000L), ops.get(1).ids.get(0));
    assertEquals(Long.valueOf(1009L), ops.get(1).ids.get(9));
  }

  @Test
  public void testManyTypeTransitions() throws Exception {
    // Alternating types - each should trigger a flush
    processor.processRow(createRow(RowKind.INSERT, 1L));
    processor.processRow(createRow(RowKind.DELETE, 2L));
    processor.processRow(createRow(RowKind.INSERT, 3L));
    processor.processRow(createRow(RowKind.DELETE, 4L));
    processor.processRow(createRow(RowKind.INSERT, 5L));
    processor.flush();

    // Should have 5 operations (each triggers a flush on type transition)
    List<Operation> ops = testIngestor.getOperations();
    assertEquals(5, ops.size());

    // Verify alternating pattern
    assertEquals(OperationType.ADD, ops.get(0).type);
    assertEquals(OperationType.DELETE, ops.get(1).type);
    assertEquals(OperationType.ADD, ops.get(2).type);
    assertEquals(OperationType.DELETE, ops.get(3).type);
    assertEquals(OperationType.ADD, ops.get(4).type);
  }

  private InternalRow createRow(RowKind kind, Long photoId) {
    GenericRow row = GenericRow.of(photoId);
    row.setRowKind(kind);
    return row;
  }

  // Test Ingestor that tracks all operations in order
  private static class TestIngestor implements Ingestor {
    private final List<Operation> operations = new ArrayList<>();
    private int commitCount = 0;

    @Override
    public void initialize(GlobalState globalState) {}

    @Override
    public void start() throws IOException {}

    @Override
    public void stop() throws IOException {}

    @Override
    public long addDocuments(List<AddDocumentRequest> addDocRequests, String indexName)
        throws Exception {
      List<Long> ids =
          addDocRequests.stream()
              .map(
                  req -> {
                    String idStr = req.getFieldsMap().get("photo_id").getValue(0);
                    return Long.parseLong(idStr);
                  })
              .collect(Collectors.toList());
      operations.add(new Operation(OperationType.ADD, ids));
      return operations.size();
    }

    @Override
    public long deleteByQuery(List<Query> queries, String indexName) throws Exception {
      List<Long> ids = new ArrayList<>();
      for (Query query : queries) {
        if (query.hasTermInSetQuery()) {
          TermInSetQuery termQuery = query.getTermInSetQuery();
          if (termQuery.hasTextTerms()) {
            ids.addAll(
                termQuery.getTextTerms().getTermsList().stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toList()));
          }
        }
      }
      operations.add(new Operation(OperationType.DELETE, ids));
      return operations.size();
    }

    @Override
    public void commit(String indexName) throws IOException {
      commitCount++;
    }

    public List<Operation> getOperations() {
      return operations;
    }

    public int getCommitCount() {
      return commitCount;
    }
  }

  // Test converter that creates simple AddDocumentRequests
  private static class TestConverter implements PaimonRowProcessor.RowConverter {
    @Override
    public AddDocumentRequest convertRowToDocument(InternalRow row) {
      Long photoId = row.getLong(0);
      return AddDocumentRequest.newBuilder()
          .setIndexName("test_index")
          .putFields(
              "photo_id",
              AddDocumentRequest.MultiValuedField.newBuilder()
                  .addValue(String.valueOf(photoId))
                  .build())
          .build();
    }
  }

  private static class Operation {
    final OperationType type;
    final List<Long> ids;

    Operation(OperationType type, List<Long> ids) {
      this.type = type;
      this.ids = ids;
    }

    @Override
    public String toString() {
      return type + ": " + ids;
    }
  }

  private enum OperationType {
    ADD,
    DELETE
  }
}
