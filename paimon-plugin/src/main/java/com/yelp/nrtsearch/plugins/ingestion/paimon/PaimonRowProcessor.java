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

import com.yelp.nrtsearch.plugins.ingestion.paimon.PaimonToAddDocumentConverter.UnrecoverableConversionException;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes Paimon rows with different RowKinds (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) and
 * ensures correct ordering while batching consecutive operations for performance.
 *
 * <p>Key behaviors:
 *
 * <ul>
 *   <li>UPDATE_BEFORE rows are skipped (UPDATE_AFTER uses Lucene's updateDocument)
 *   <li>Consecutive operations of the same type are batched together
 *   <li>Type transitions (DELETE→ADD or ADD→DELETE) trigger flushes to preserve ordering
 *   <li>Final flush ensures all operations are executed in correct order
 * </ul>
 */
public class PaimonRowProcessor {
  private static final Logger logger = LoggerFactory.getLogger(PaimonRowProcessor.class);

  private final String indexName;
  private final IdFieldInitializer idFieldInitializer;
  private final RowConverter converter;
  private final Ingestor ingestor;
  private final RowType rowType;

  private final List<AddDocumentRequest> addBatch = new ArrayList<>();
  private final List<Object> deleteBatch = new ArrayList<>();
  private OperationType lastOp = OperationType.NONE;

  enum OperationType {
    DELETE,
    ADD,
    NONE
  }

  /** Functional interface for converting Paimon rows to AddDocumentRequests */
  public interface RowConverter {
    AddDocumentRequest convertRowToDocument(InternalRow row) throws Exception;
  }

  /** Functional interface for lazy ID field initialization */
  public interface IdFieldInitializer {
    IdFieldInfo initializeIdField() throws Exception;
  }

  /** Container for ID field metadata */
  public static class IdFieldInfo {
    public final String fieldName;
    public final int fieldIndex;

    public IdFieldInfo(String fieldName, int fieldIndex) {
      this.fieldName = fieldName;
      this.fieldIndex = fieldIndex;
    }
  }

  public PaimonRowProcessor(
      String indexName,
      IdFieldInitializer idFieldInitializer,
      RowConverter converter,
      Ingestor ingestor,
      RowType rowType) {
    this.indexName = indexName;
    this.idFieldInitializer = idFieldInitializer;
    this.converter = converter;
    this.ingestor = ingestor;
    this.rowType = rowType;
  }

  /**
   * Process a single Paimon row, batching when safe and flushing on type transitions.
   *
   * @param row the Paimon row to process
   * @throws Exception if indexing or deletion fails
   */
  public void processRow(InternalRow row) throws Exception {
    RowKind kind = row.getRowKind();

    switch (kind) {
      case UPDATE_BEFORE:
        // Skip UPDATE_BEFORE - let UPDATE_AFTER handle it via Lucene's updateDocument
        // Generally we use changelog-producer.row-deduplicate option in paimon to not generate 2x
        // messages for updates so upsert mode is fine i.e. only after image
        // TODO: Sometimes we might need to send -U (deletes) for these to nrtsearch but doing so on
        // all -U,+U updates causes 2x indexing load.
        break;

      case UPDATE_AFTER:
      case INSERT:
        // Flush deletes if switching from DELETE to ADD
        if (lastOp == OperationType.DELETE && !deleteBatch.isEmpty()) {
          flushDeletes();
        }
        // Batch the add/update with poison pill protection
        AddDocumentRequest docRequest;
        try {
          docRequest = converter.convertRowToDocument(row);
        } catch (UnrecoverableConversionException e) {
          // POISON PILL: Log and skip bad record, continue processing
          logger.error("Poison pill detected - skipping bad record: {}", e.getMessage());
          break; // Skip this record
        }
        addBatch.add(docRequest);
        lastOp = OperationType.ADD;
        break;

      case DELETE:
        // Flush adds if switching from ADD to DELETE
        if (lastOp == OperationType.ADD && !addBatch.isEmpty()) {
          flushAdds();
        }
        // Batch the delete (initializer handles caching internally)
        IdFieldInfo idInfo = idFieldInitializer.initializeIdField();
        deleteBatch.add(extractIdValue(row, idInfo.fieldIndex));
        lastOp = OperationType.DELETE;
        break;

      default:
        logger.warn("Unknown RowKind: {}, skipping row", kind);
    }
  }

  /**
   * Flush any remaining batched operations in correct order.
   *
   * @throws Exception if indexing or deletion fails
   */
  public void flush() throws Exception {
    boolean hasDeletes = !deleteBatch.isEmpty();
    boolean hasAdds = !addBatch.isEmpty();

    if (!hasDeletes && !hasAdds) {
      return;
    }

    // Flush in order based on what came last
    if (lastOp == OperationType.DELETE) {
      if (hasAdds) {
        // This should not happen due to type-transition flushing, but be defensive
        logger.warn("addBatch not empty when lastOp is DELETE, flushing out of order");
        flushAdds();
      }
      if (hasDeletes) {
        flushDeletes();
      }
    } else if (lastOp == OperationType.ADD) {
      if (hasDeletes) {
        // This should not happen due to type-transition flushing, but be defensive
        logger.warn("deleteBatch not empty when lastOp is ADD, flushing out of order");
        flushDeletes();
      }
      if (hasAdds) {
        flushAdds();
      }
    }
  }

  private void flushDeletes() throws Exception {
    IdFieldInfo idInfo = idFieldInitializer.initializeIdField();
    Query deleteQuery = buildDeleteQuery(deleteBatch, idInfo.fieldName);
    ingestor.deleteByQuery(List.of(deleteQuery), indexName);
    ingestor.commit(indexName);
    logger.debug("Flushed {} deletes", deleteBatch.size());
    deleteBatch.clear();
  }

  private void flushAdds() throws Exception {
    ingestor.addDocuments(addBatch, indexName);
    ingestor.commit(indexName);
    logger.debug("Flushed {} adds", addBatch.size());
    addBatch.clear();
  }

  private Object extractIdValue(InternalRow row, int fieldIndex) {
    if (row.isNullAt(fieldIndex)) {
      throw new IllegalStateException("ID field cannot be null for delete operation");
    }

    // Get the Paimon field type from schema
    DataType dataType = rowType.getFields().get(fieldIndex).type();

    // nrtSearch _ID fields are always STRING type, convert based on Paimon type
    switch (dataType.getTypeRoot()) {
      case BOOLEAN:
        return String.valueOf(row.getBoolean(fieldIndex));
      case TINYINT:
        return String.valueOf(row.getByte(fieldIndex));
      case SMALLINT:
        return String.valueOf(row.getShort(fieldIndex));
      case INTEGER:
        return String.valueOf(row.getInt(fieldIndex));
      case BIGINT:
        return String.valueOf(row.getLong(fieldIndex));
      case FLOAT:
        return String.valueOf(row.getFloat(fieldIndex));
      case DOUBLE:
        return String.valueOf(row.getDouble(fieldIndex));
      case CHAR:
      case VARCHAR:
        return row.getString(fieldIndex).toString();
      default:
        throw new IllegalStateException(
            "Unsupported _ID field type: "
                + dataType.getTypeRoot()
                + " at index "
                + fieldIndex
                + ". _ID fields must be numeric or string types.");
    }
  }

  private Query buildDeleteQuery(List<Object> ids, String fieldName) {
    if (ids.isEmpty()) {
      throw new IllegalArgumentException("Cannot build delete query with empty ID list");
    }

    // nrtSearch _ID fields are always STRING type
    @SuppressWarnings("unchecked")
    List<String> stringIds = (List<String>) (List<?>) ids;

    return Query.newBuilder()
        .setTermInSetQuery(
            TermInSetQuery.newBuilder()
                .setField(fieldName)
                .setTextTerms(TermInSetQuery.TextTerms.newBuilder().addAllTerms(stringIds).build())
                .build())
        .build();
  }
}
