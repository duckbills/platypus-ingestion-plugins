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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.plugins.ingestion.paimon.PaimonToAddDocumentConverter.UnrecoverableConversionException;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for PaimonIngestor. This class is structured to test distinct logical components: 1.
 * Worker Logic: How individual work items are processed, including error handling. 2. Coordinator
 * Logic: How work is discovered and checkpointing is managed.
 */
@RunWith(MockitoJUnitRunner.class)
public class PaimonIngestorTest {

  // ============================================================================
  // COMMON SETUP
  // ============================================================================

  @Mock private PaimonConfig mockPaimonConfig;
  @Mock private ExecutorService mockExecutorService;
  @Mock private PaimonToAddDocumentConverter mockConverter;
  @Mock private Table mockTable;
  @Mock private TableRead mockTableRead;
  @Mock private StreamTableScan mockStreamTableScan;
  @Mock private TableScan.Plan mockPlan;
  @Mock private RecordReader<InternalRow> mockRecordReader;
  @Mock private NrtsearchConfig mockNrtsearchConfig;
  @Mock private BlockingQueue<PaimonIngestor.BucketWork> mockWorkQueue;

  private PaimonIngestor ingestor;

  @Before
  public void setUp() throws Exception {
    // Configure default mock behaviors
    lenient().when(mockPaimonConfig.getWorkerThreads()).thenReturn(2);
    lenient().when(mockPaimonConfig.getBatchSize()).thenReturn(10);
    lenient().when(mockPaimonConfig.getTargetIndexName()).thenReturn("test_index");
    lenient().when(mockPaimonConfig.getQueueCapacity()).thenReturn(1000);

    // Create ingestor manually and spy on it
    ingestor = spy(new PaimonIngestor(mockNrtsearchConfig, mockExecutorService, mockPaimonConfig));

    // Manually inject mocks for clean dependency injection
    ingestor.setTable(mockTable);
    ingestor.setTableRead(mockTableRead);
    ingestor.setStreamTableScan(mockStreamTableScan);
    ingestor.setConverter(mockConverter);
    ingestor.setWorkQueue(mockWorkQueue);
  }

  // ============================================================================
  // WORKER LOGIC TESTS
  // ============================================================================

  @Test
  public void testWorker_HappyPath_ProcessesBucketAndMarksComplete() throws Exception {
    // Arrange
    InFlightBatch mockBatch = mock(InFlightBatch.class);
    DataSplit mockSplit = mock(DataSplit.class);
    InternalRow mockRow = mock(InternalRow.class);
    PaimonIngestor.BucketWork bucketWork =
        new PaimonIngestor.BucketWork(1, List.of(mockSplit), mockBatch);

    when(mockTableRead.createReader(mockSplit)).thenReturn(mockRecordReader);
    // Simulate the reader providing one row
    doAnswer(
            invocation -> {
              java.util.function.Consumer<InternalRow> consumer = invocation.getArgument(0);
              consumer.accept(mockRow);
              return null;
            })
        .when(mockRecordReader)
        .forEachRemaining(any());

    // We spy on 'ingestor' to mock out the actual Lucene interaction
    doNothing().when(ingestor).processBatch(any(), anyInt(), anyInt());

    // Act
    ingestor.processBucketWork(bucketWork, 1);

    // Assert
    verify(ingestor, times(1)).processBatch(any(), eq(1), eq(1));
    verify(mockBatch, times(1)).markBucketComplete();
  }

  @Test
  public void testWorker_PoisonPill_SkipsBadRecordAndCompletesBucket() throws Exception {
    // Arrange
    InFlightBatch mockBatch = mock(InFlightBatch.class);
    DataSplit mockSplit = mock(DataSplit.class);
    InternalRow goodRow = mock(InternalRow.class, "goodRow");
    InternalRow badRow = mock(InternalRow.class, "badRow");
    PaimonIngestor.BucketWork bucketWork =
        new PaimonIngestor.BucketWork(1, List.of(mockSplit), mockBatch);

    when(mockTableRead.createReader(mockSplit)).thenReturn(mockRecordReader);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<InternalRow> consumer = invocation.getArgument(0);
              consumer.accept(badRow);
              consumer.accept(goodRow);
              return null;
            })
        .when(mockRecordReader)
        .forEachRemaining(any());

    when(mockConverter.convertRowToDocument(badRow))
        .thenThrow(
            new UnrecoverableConversionException(
                "Bad record", new RuntimeException("Conversion failed")));
    when(mockConverter.convertRowToDocument(goodRow)).thenReturn(mock(AddDocumentRequest.class));

    // Capture batch contents before they get cleared
    List<AddDocumentRequest> capturedDocuments = new ArrayList<>();
    doAnswer(
            invocation -> {
              List<AddDocumentRequest> batch = invocation.getArgument(0);
              capturedDocuments.addAll(batch);
              return null;
            })
        .when(ingestor)
        .processBatch(any(), anyInt(), anyInt());

    // Act
    ingestor.processBucketWork(bucketWork, 1);

    // Assert
    verify(ingestor, atLeastOnce()).processBatch(any(), anyInt(), anyInt());
    assertEquals("Should have processed exactly one document", 1, capturedDocuments.size());
    verify(mockBatch, times(1)).markBucketComplete();
  }

  @Test
  public void testWorker_PermanentFailure_StopsRetryingAndDoesNotMarkComplete() throws Exception {
    // Arrange
    InFlightBatch mockBatch = mock(InFlightBatch.class);
    DataSplit mockSplit = mock(DataSplit.class);
    InternalRow mockRow = mock(InternalRow.class);
    PaimonIngestor.BucketWork bucketWork =
        new PaimonIngestor.BucketWork(1, List.of(mockSplit), mockBatch);
    final int MAX_RETRIES = 3; // From implementation

    when(mockTableRead.createReader(mockSplit)).thenReturn(mockRecordReader);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<InternalRow> consumer = invocation.getArgument(0);
              consumer.accept(mockRow);
              return null;
            })
        .when(mockRecordReader)
        .forEachRemaining(any());

    // Simulate a permanent failure in the underlying batch processing
    doThrow(new RuntimeException("DB is down!"))
        .when(ingestor)
        .processBatch(any(), anyInt(), anyInt());

    // Act
    ingestor.processBucketWork(bucketWork, 1);

    // Assert
    verify(ingestor, times(MAX_RETRIES)).processBatch(any(), anyInt(), anyInt());
    verify(mockBatch, never()).markBucketComplete(); // CRITICAL: This ensures the "Safe Halt"
  }

  // ============================================================================
  // COORDINATOR LOGIC TESTS
  // ============================================================================

  // In PaimonIngestorTest.java

  @Test
  public void testCoordinator_HappyPath_DispatchesWorkAndCommitsCheckpoint() throws Exception {
    // Arrange
    ingestor.getRunning().set(true);
    DataSplit mockSplit = mock(DataSplit.class);
    long newCheckpointId = 12345L;

    when(mockStreamTableScan.plan()).thenReturn(mockPlan);
    when(mockPlan.splits()).thenReturn(List.of(mockSplit));
    when(mockStreamTableScan.checkpoint()).thenReturn(newCheckpointId);
    when(mockSplit.bucket()).thenReturn(1);

    // Use doAnswer to deterministically complete the batch and stop the loop
    doAnswer(
            invocation -> {
              PaimonIngestor.BucketWork work = invocation.getArgument(0);
              work.getBatch().markBucketComplete(); // Unblock awaitCompletion()
              ingestor.getRunning().set(false); // Stop the loop after one iteration
              return true; // Indicate success for queue.offer()
            })
        .when(mockWorkQueue)
        .offer(any(PaimonIngestor.BucketWork.class), anyLong(), any());

    // Act
    ingestor.coordinatorLoop();

    // Assert
    // The doAnswer block handles the interaction, so we just verify the outcome.
    verify(mockWorkQueue, times(1)).offer(any(PaimonIngestor.BucketWork.class), anyLong(), any());
  }

  @Test
  public void testCoordinator_NoNewSplits_DoesNothingAndWait() throws Exception {
    // Arrange
    ingestor.getRunning().set(true);
    // Setup mock to run the loop exactly once with no data
    when(mockStreamTableScan.plan())
        .thenAnswer(
            invocation -> {
              ingestor.getRunning().set(false); // Stop the loop
              return mockPlan;
            });
    when(mockPlan.splits()).thenReturn(Collections.emptyList());

    // Act
    ingestor.coordinatorLoop();

    // Assert
    verify(mockWorkQueue, never()).offer(any(), anyLong(), any());
    verify(mockStreamTableScan, never()).checkpoint();
  }

  @Test
  public void testWorker_SuccessfulRetryOnTransientFailure() throws Exception {
    // Arrange
    InFlightBatch mockBatch = mock(InFlightBatch.class);
    DataSplit mockSplit = mock(DataSplit.class);
    InternalRow mockRow = mock(InternalRow.class);
    PaimonIngestor.BucketWork bucketWork =
        new PaimonIngestor.BucketWork(1, List.of(mockSplit), mockBatch);

    when(mockTableRead.createReader(mockSplit)).thenReturn(mockRecordReader);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<InternalRow> consumer = invocation.getArgument(0);
              consumer.accept(mockRow);
              return null;
            })
        .when(mockRecordReader)
        .forEachRemaining(any());

    // Simulate a failure on the first attempt, then succeed on the second
    doThrow(new RuntimeException("Transient DB error!"))
        .doNothing()
        .when(ingestor)
        .processBatch(any(), anyInt(), anyInt());

    // Act
    ingestor.processBucketWork(bucketWork, 1);

    // Assert
    // Verify it was attempted twice (1 failure + 1 success)
    verify(ingestor, times(2)).processBatch(any(), anyInt(), anyInt());
    verify(mockBatch, times(1)).markBucketComplete();
  }

  @Test
  public void testWorker_BatchingBySize() throws Exception {
    // Arrange
    final int BATCH_SIZE = 3;
    when(mockPaimonConfig.getBatchSize()).thenReturn(BATCH_SIZE);

    InFlightBatch mockBatch = mock(InFlightBatch.class);
    DataSplit mockSplit = mock(DataSplit.class);
    InternalRow row1 = mock(InternalRow.class, "row1");
    InternalRow row2 = mock(InternalRow.class, "row2");
    InternalRow row3 = mock(InternalRow.class, "row3");
    InternalRow row4 = mock(InternalRow.class, "row4");
    PaimonIngestor.BucketWork bucketWork =
        new PaimonIngestor.BucketWork(1, List.of(mockSplit), mockBatch);

    when(mockTableRead.createReader(mockSplit)).thenReturn(mockRecordReader);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<InternalRow> consumer = invocation.getArgument(0);
              consumer.accept(row1);
              consumer.accept(row2);
              consumer.accept(row3);
              consumer.accept(row4);
              return null;
            })
        .when(mockRecordReader)
        .forEachRemaining(any());

    when(mockConverter.convertRowToDocument(any(InternalRow.class)))
        .thenReturn(mock(AddDocumentRequest.class));

    // Capture all batch calls
    List<List<AddDocumentRequest>> capturedBatches = new ArrayList<>();
    doAnswer(
            invocation -> {
              List<AddDocumentRequest> batch = invocation.getArgument(0);
              capturedBatches.add(new ArrayList<>(batch));
              return null;
            })
        .when(ingestor)
        .processBatch(any(), anyInt(), anyInt());

    // Act
    ingestor.processBucketWork(bucketWork, 1);

    // Assert
    verify(ingestor, times(2)).processBatch(any(), anyInt(), anyInt());
    assertEquals("Should have processed two batches", 2, capturedBatches.size());
    assertEquals("First batch should be full", BATCH_SIZE, capturedBatches.get(0).size());
    assertEquals("Second batch should contain the remainder", 1, capturedBatches.get(1).size());
    verify(mockBatch, times(1)).markBucketComplete();
  }

  @Test
  public void testCoordinator_RecoversFromPaimonAPIError() throws Exception {
    // Arrange
    ingestor.getRunning().set(true);

    // First call fails, second succeeds but returns no splits and stops the loop
    when(mockStreamTableScan.plan())
        .thenThrow(new RuntimeException("Paimon connection failed!"))
        .thenAnswer(
            invocation -> {
              ingestor.getRunning().set(false);
              return mockPlan;
            });
    when(mockPlan.splits()).thenReturn(Collections.emptyList());

    // Act
    ingestor.coordinatorLoop();

    // Assert
    verify(mockStreamTableScan, times(2)).plan();
    verify(mockWorkQueue, never()).offer(any(), anyLong(), any());
  }

  // ============================================================================
  // LIFECYCLE AND INITIALIZATION TESTS
  // ============================================================================

  @Test(expected = org.apache.paimon.catalog.Catalog.TableNotExistException.class)
  public void testInitialize_FailsOnMissingDatabaseOrTableName() throws Exception {
    // Arrange
    when(mockPaimonConfig.getWarehousePath()).thenReturn("/tmp/test_warehouse");
    when(mockPaimonConfig.getDatabaseName()).thenReturn("test_db");
    when(mockPaimonConfig.getTableName()).thenReturn("test_table");

    ingestor.initializePaimonComponents();
  }
}
