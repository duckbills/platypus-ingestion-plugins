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

import com.yelp.nrtsearch.plugins.ingestion.paimon.sharding.ShardingStrategy;
import com.yelp.nrtsearch.plugins.ingestion.paimon.sharding.ShardingStrategyFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.ingestion.AbstractIngestor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingestor for Apache Paimon data lake tables using Dynamic Shared Queue architecture. Handles
 * workload skew in dynamic bucket tables by using a coordinator-worker pattern with incremental
 * checkpointing and ordering guarantees for same keys.
 */
public class PaimonIngestor extends AbstractIngestor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PaimonIngestor.class);
  private final NrtsearchConfig nrtSearchConfig;

  private BlockingQueue<BucketWork> workQueue;
  private final ExecutorService executorService;
  private final AtomicBoolean running;
  private final PaimonConfig paimonConfig;
  private PaimonToAddDocumentConverter converter;

  // _ID field metadata (lazy initialized)
  private volatile String idFieldName;
  private volatile int paimonIdFieldIndex = -1;
  private final Object idFieldInitLock = new Object();

  // Statistics tracking
  private final AtomicLong totalDocumentsProcessed = new AtomicLong(0);
  private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
  private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
  private volatile long lastStatsLogTime = System.currentTimeMillis();

  // Paimon components
  private Catalog catalog;
  private Table table;
  private TableRead tableRead;
  private StreamTableScan streamTableScan;

  public PaimonIngestor(
      NrtsearchConfig config, ExecutorService executorService, PaimonConfig paimonConfig) {
    super(config);
    this.executorService = executorService;
    this.workQueue = new LinkedBlockingQueue<>(paimonConfig.getQueueCapacity());
    this.running = new AtomicBoolean(false);
    this.paimonConfig = paimonConfig;
    this.nrtSearchConfig = config;
    this.converter = new PaimonToAddDocumentConverter(paimonConfig);

    LOGGER.info(
        "Initialized PaimonIngestor with Dynamic Shared Queue architecture and incremental checkpointing");
  }

  @Override
  public void start() throws IOException {
    if (running.compareAndSet(false, true)) {
      // Set this thread's name to coordinator for clear logging
      Thread.currentThread().setName("paimon-coordinator-" + nrtSearchConfig.getServiceName());

      LOGGER.info("Starting Paimon ingestion");

      try {
        initializePaimonComponents();

        // Start worker threads with explicit names
        for (int i = 0; i < paimonConfig.getWorkerThreads(); i++) {
          final int workerId = i;
          executorService.submit(
              () -> {
                Thread.currentThread().setName("paimon-worker-" + workerId);
                workerLoop(workerId);
              });
        }

        LOGGER.info(
            "Paimon ingestion started with {} workers, coordinator thread starting",
            paimonConfig.getWorkerThreads());

        // This thread now runs the coordinator loop directly (blocking until shutdown)
        coordinatorLoop();

      } catch (Exception e) {
        running.set(false);
        throw new IOException("Failed to start Paimon ingestion", e);
      }
    }
  }

  @Override
  public void stop() throws IOException {
    if (running.compareAndSet(true, false)) {
      LOGGER.info("Stopping Paimon ingestion");

      try {
        if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        executorService.shutdownNow();
      }

      // Close Paimon resources
      try {
        if (catalog != null) {
          catalog.close();
        }
      } catch (Exception e) {
        LOGGER.error("Error closing Paimon catalog", e);
      }

      LOGGER.info("Paimon ingestion stopped");
    }
  }

  /** Initialize Paimon catalog, table, and read components. */
  protected void initializePaimonComponents() throws Exception {
    LOGGER.info("Initializing Paimon components");

    // Create catalog options
    Options catalogOptions = new Options();
    catalogOptions.set("warehouse", paimonConfig.getWarehousePath());

    // --- ADD S3 CONFIGURATION TRANSLATION ---
    // Check if warehouse path uses S3 - if so, apply S3 settings regardless of explicit config
    if (paimonConfig.getWarehousePath().startsWith("s3a://")) {
      LOGGER.info("S3A warehouse path detected. Applying S3A configuration.");

      // Universal S3A settings - needed for ANY S3A access (test or production)
      catalogOptions.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      catalogOptions.set("fs.s3a.connection.maximum", "256");
      catalogOptions.set("fs.s3a.threads.max", "128");
      catalogOptions.set("fs.s3a.block.size", "64M");

      // Get optional S3 configuration for environment-specific settings
      Map<String, Map<String, Object>> pluginConfigs = config.getIngestionPluginConfigs();
      Map<String, Object> paimonPluginConfig = pluginConfigs.get("paimon");
      Map<String, Object> s3Config = null;
      if (paimonPluginConfig != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> s3ConfigRaw = (Map<String, Object>) paimonPluginConfig.get("s3");
        s3Config = s3ConfigRaw;
      }

      if (s3Config != null && s3Config.get("endpoint") != null) {
        // --- TEST/LOCAL ENVIRONMENT ---
        // Endpoint provided = using S3Mock, configure with explicit credentials
        LOGGER.info("S3 endpoint provided. Configuring for local/test environment.");
        catalogOptions.set("fs.s3a.endpoint", s3Config.get("endpoint").toString());
        catalogOptions.set("fs.s3a.access.key", s3Config.get("s3-access-key").toString());
        catalogOptions.set("fs.s3a.secret.key", s3Config.get("s3-secret-key").toString());
        if ("true".equals(s3Config.get("path.style.access"))) {
          catalogOptions.set("fs.s3a.path.style.access", "true");
        }
        catalogOptions.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      } else {
        // --- PRODUCTION ENVIRONMENT ---
        // No explicit S3 config = production with IAM roles
        LOGGER.info("No S3 endpoint configured. Using production IAM roles.");
        catalogOptions.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
      }

      LOGGER.info("Applied S3A configuration for warehouse: {}", paimonConfig.getWarehousePath());
    }
    // --- END S3 CONFIGURATION TRANSLATION ---

    // Hadoop dependencies should be on the host classpath (nrtsearch/lib)
    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
    this.catalog = CatalogFactory.createCatalog(catalogContext);

    String database = paimonConfig.getDatabaseName();
    String tableName = paimonConfig.getTableName();

    Table baseTable = catalog.getTable(Identifier.create(database, tableName));
    Map<String, String> consumerOptions = new HashMap<>();
    consumerOptions.put(CoreOptions.CONSUMER_ID.key(), nrtSearchConfig.getServiceName());
    this.table = ((FileStoreTable) baseTable).copy(consumerOptions);
    // Initialize converter with table schema
    converter.setRowType(table.rowType());

    // Create and apply sharding strategy
    ShardingStrategy shardingStrategy =
        ShardingStrategyFactory.create(paimonConfig.getShardingConfig());
    shardingStrategy.validateTable(table);
    LOGGER.info("Sharding strategy: {}", shardingStrategy.getDescription());

    // Apply sharding to read builder
    ReadBuilder readBuilder = table.newReadBuilder();
    readBuilder =
        shardingStrategy.applySharding(readBuilder, table, nrtSearchConfig.getServiceName());

    // Create table read and stream scan
    this.streamTableScan = readBuilder.newStreamScan();
    this.tableRead = readBuilder.newRead();

    LOGGER.info("Successfully initialized Paimon table: {}", paimonConfig.getTablePath());
  }

  /**
   * Coordinator thread: discovers incremental work and distributes to shared queue. Uses Paimon's
   * checkpoint/restore mechanism for incremental processing.
   */
  protected void coordinatorLoop() {
    LOGGER.info("Coordinator thread started with incremental checkpointing");

    while (running.get()) {
      try {
        // Scan for new splits since last checkpoint
        List<Split> incrementalSplits = streamTableScan.plan().splits();

        LOGGER.debug("Found {} incremental splits since last checkpoint", incrementalSplits.size());

        if (!incrementalSplits.isEmpty()) {
          // Group splits by bucket for ordered processing
          Map<Integer, List<Split>> splitsByBucket = groupSplitsByBucket(incrementalSplits);
          long nextSnapshot = streamTableScan.checkpoint(); // Get the nextSnapshot ID upfront

          // PHASE 1: PREPARE BATCH - Create coordination object
          InFlightBatch batch = new InFlightBatch(nextSnapshot, splitsByBucket.size());

          // PHASE 2: DISPATCH WORK - Add all work to queue with batch reference
          long totalSplitSize = 0;
          for (Map.Entry<Integer, List<Split>> entry : splitsByBucket.entrySet()) {
            List<Split> sortedSplits = sortSplitsBySequence(entry.getValue());
            BucketWork bucketWork = new BucketWork(entry.getKey(), sortedSplits, batch);

            // Calculate split sizes for logging
            long bucketSplitSize = 0;
            for (Split split : sortedSplits) {
              if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                bucketSplitSize +=
                    dataSplit.dataFiles().stream().mapToLong(file -> file.fileSize()).sum();
              }
            }
            totalSplitSize += bucketSplitSize;

            LOGGER.debug(
                "Bucket {} has {} splits, total size: {:.2f} MB",
                entry.getKey(),
                sortedSplits.size(),
                bucketSplitSize / (1024.0 * 1024.0));

            // Block with timeout and continuous logging (prevents data loss)
            boolean queued = false;
            while (!queued && running.get()) {
              try {
                if (workQueue.offer(bucketWork, 30, TimeUnit.SECONDS)) {
                  queued = true;
                } else {
                  LOGGER.warn(
                      "Work queue full for 30s, coordinator blocked - no progress on bucket {} with {} splits",
                      entry.getKey(),
                      entry.getValue().size());
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }

          LOGGER.info(
              "Coordinator dispatched {} buckets ({} total splits, {:.2f} MB data) for checkpoint {}. Awaiting completion...",
              splitsByBucket.size(),
              incrementalSplits.size(),
              totalSplitSize / (1024.0 * 1024.0),
              nextSnapshot);

          // PHASE 3: AWAIT COMPLETION - Critical synchronization point

          long batchStartTime = System.currentTimeMillis();
          batch.awaitCompletion();
          long batchDuration = System.currentTimeMillis() - batchStartTime;

          LOGGER.info(
              "All workers completed checkpoint {} in {}s - {} buckets processed",
              nextSnapshot,
              String.format("%.1f", batchDuration / 1000.0),
              splitsByBucket.size());

          // PHASE 4: COMMIT CHECKPOINT - Only happens after all work is done
          streamTableScan.notifyCheckpointComplete(nextSnapshot);

          LOGGER.info(
              "Committed Paimon checkpoint {} - total processing time: {}s",
              nextSnapshot,
              String.format("%.1f", (System.currentTimeMillis() - batchStartTime) / 1000.0));
        }

        // Sleep before next scan
        Thread.sleep(paimonConfig.getScanIntervalMs());

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOGGER.error("Error in coordinator loop", e);
        try {
          Thread.sleep(5000); // Back off on error
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    LOGGER.info("Coordinator thread stopped");
  }

  /**
   * Worker thread: processes buckets from shared queue. Maintains ordering guarantees within each
   * bucket while allowing parallel processing.
   */
  protected void workerLoop(int workerId) {
    LOGGER.info("Worker thread {} started", workerId);

    while (running.get()) {
      try {
        BucketWork bucketWork =
            workQueue.poll(paimonConfig.getPollTimeoutMs(), TimeUnit.MILLISECONDS);
        if (bucketWork != null) {
          processBucketWork(bucketWork, workerId); // Never throws - handles all retries internally
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      // No catch (Exception e) needed - processBucketWork never throws
    }

    LOGGER.info("Worker thread {} stopped", workerId);
  }

  /**
   * Process a single bucket's worth of work. Maintains sequential ordering within the bucket for
   * same keys. Retries indefinitely with exponential backoff until success or shutdown.
   */
  protected void processBucketWork(BucketWork bucketWork, int workerId) {
    int attempt = 1;
    long backoffMs = 100; // Start with 100ms for faster retries during index initialization

    while (running.get()) {
      try {
        processBucketAtomically(bucketWork, workerId, attempt);

        // SUCCESS: Signal completion and exit
        bucketWork.getBatch().markBucketComplete();

        // Calculate bucket processing stats
        long bucketDataSize = 0;
        int totalDocuments = 0;
        for (Split split : bucketWork.getSplits()) {
          if (split instanceof DataSplit) {
            DataSplit dataSplit = (DataSplit) split;
            bucketDataSize +=
                dataSplit.dataFiles().stream().mapToLong(file -> file.fileSize()).sum();
            totalDocuments += dataSplit.rowCount();
          }
        }

        if (attempt == 1) {
          LOGGER.info(
              "Worker {} completed bucket {} - {} splits, {} MB, ~{} docs",
              workerId,
              bucketWork.getBucketId(),
              bucketWork.getSplits().size(),
              String.format("%.2f", bucketDataSize / (1024.0 * 1024.0)),
              totalDocuments);
        } else {
          LOGGER.info(
              "Worker {} completed bucket {} after {} attempts - {} splits, {} MB, ~{} docs",
              workerId,
              bucketWork.getBucketId(),
              attempt,
              bucketWork.getSplits().size(),
              String.format("%.2f", bucketDataSize / (1024.0 * 1024.0)),
              totalDocuments);
        }
        return;

      } catch (Exception e) {
        // ALL exceptions are treated as transient - log and retry with backoff
        LOGGER.warn(
            "Worker {} attempt {} failed for bucket {} (next backoff: {}ms): {}",
            workerId,
            attempt,
            bucketWork.getBucketId(),
            backoffMs,
            e.getMessage());
        LOGGER.debug(
            "Full exception stack trace for bucket {} attempt {}:",
            bucketWork.getBucketId(),
            attempt,
            e);

        // Sleep with exponential backoff (capped at 1 minute)
        sleepWithBackoff(backoffMs);
        backoffMs = Math.min(backoffMs * 2, 60000);
        attempt++;
      }
    }

    // Only exit if shutdown was requested
    LOGGER.info(
        "Worker {} stopped processing bucket {} due to shutdown after {} attempts",
        workerId,
        bucketWork.getBucketId(),
        attempt - 1);
  }

  /** Atomic processing of all splits in a bucket with ordering guarantees. */
  private void processBucketAtomically(BucketWork bucketWork, int workerId, int attempt)
      throws Exception {
    LOGGER.debug(
        "Worker {} processing bucket {} attempt {}", workerId, bucketWork.getBucketId(), attempt);

    // Create PaimonRowProcessor for this bucket - handles ordering and batching
    String indexName = paimonConfig.getTargetIndexName();
    PaimonRowProcessor processor =
        new PaimonRowProcessor(
            indexName, this::ensureIdFieldInitialized, converter, this, table.rowType());

    // Process each split sequentially to maintain ordering
    for (Split split : bucketWork.getSplits()) {
      LOGGER.debug("Worker {} processing split: {}", workerId, split);

      try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
        LOGGER.debug(
            "Created RecordReader for split - reader class: {}", reader.getClass().getSimpleName());

        AtomicLong rowCount = new AtomicLong(0);
        reader.forEachRemaining(
            row -> {
              try {
                long currentRowNum = rowCount.incrementAndGet();

                // Log every 1000th row to track progress and show filtering is working
                if (currentRowNum % 1000 == 0) {
                  LOGGER.debug(
                      "Worker {} processed {} rows from current split so far",
                      workerId,
                      currentRowNum);
                }

                processor.processRow(row);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

        LOGGER.debug(
            "Worker {} completed split - processed {} total rows", workerId, rowCount.get());
      }
    }

    // Flush any remaining operations
    processor.flush();

    LOGGER.debug("Worker {} completed bucket {}", workerId, bucketWork.getBucketId());
  }

  /**
   * Lazy initialization of ID field metadata using double-checked locking.
   *
   * @return IdFieldInfo containing field name and Paimon column index
   * @throws Exception if index doesn't exist or doesn't have an ID field
   */
  private PaimonRowProcessor.IdFieldInfo ensureIdFieldInitialized() throws Exception {
    // Double-checked locking for thread-safe lazy initialization
    if (paimonIdFieldIndex < 0) {
      synchronized (idFieldInitLock) {
        if (paimonIdFieldIndex < 0) {
          String indexName = paimonConfig.getTargetIndexName();
          Optional<IdFieldDef> idFieldDefOpt = getIdFieldDef(indexName);
          if (idFieldDefOpt.isEmpty()) {
            throw new IllegalStateException(
                "Index "
                    + indexName
                    + " must have an _id field defined which serves as primary key");
          }

          IdFieldDef idFieldDef = idFieldDefOpt.get();
          String fieldName = idFieldDef.getName();
          int fieldIndex = table.rowType().getFieldIndex(fieldName);

          if (fieldIndex < 0) {
            throw new IllegalStateException(
                "ID field '" + fieldName + "' not found in Paimon table schema");
          }

          // Set fields atomically after validation (volatile write ensures visibility)
          this.idFieldName = fieldName;
          this.paimonIdFieldIndex = fieldIndex;

          LOGGER.info(
              "Initialized ID field '{}' at Paimon column index {} which serves as primary key",
              idFieldName,
              paimonIdFieldIndex);
        }
      }
    }
    return new PaimonRowProcessor.IdFieldInfo(idFieldName, paimonIdFieldIndex);
  }

  /** Override addDocuments to track statistics for monitoring. */
  @Override
  public long addDocuments(List<AddDocumentRequest> addDocRequests, String indexName)
      throws Exception {
    long startTime = System.currentTimeMillis();
    long seqNum = super.addDocuments(addDocRequests, indexName);
    long duration = System.currentTimeMillis() - startTime;

    // Update statistics
    totalDocumentsProcessed.addAndGet(addDocRequests.size());
    totalBatchesProcessed.incrementAndGet();
    totalProcessingTimeMs.addAndGet(duration);

    LOGGER.info(
        "Indexed {} docs in {}ms, seqNum: {}, throughput: {} docs/sec",
        addDocRequests.size(),
        duration,
        seqNum,
        String.format("%.1f", duration > 0 ? (addDocRequests.size() * 1000.0 / duration) : 0.0));

    // Log periodic summary statistics (every 5 minutes)
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastStatsLogTime > 300000) { // 5 minutes
      logSummaryStatistics();
      lastStatsLogTime = currentTime;
    }

    return seqNum;
  }

  /** Sleep with backoff for error recovery. */
  private void sleepWithBackoff(long backoffMs) {
    try {
      Thread.sleep(backoffMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Group splits by bucket ID for ordered processing. Extracts bucket information from DataSplit
   * metadata.
   */
  private Map<Integer, List<Split>> groupSplitsByBucket(List<Split> splits) {
    Map<Integer, List<Split>> bucketMap = new HashMap<>();

    for (Split split : splits) {
      int bucketId = extractBucketId(split);
      bucketMap.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(split);
    }

    return bucketMap;
  }

  /** Sort splits by snapshot ID first, then sequence number to maintain temporal ordering. */
  private List<Split> sortSplitsBySequence(List<Split> splits) {
    List<Split> sortedSplits = new ArrayList<>(splits);

    // Sort by snapshot ID first (temporal ordering), then by sequence number (LSM ordering)
    sortedSplits.sort(
        (s1, s2) -> {
          if (!(s1 instanceof DataSplit) || !(s2 instanceof DataSplit)) {
            return 0; // Non-DataSplit types are equal
          }

          DataSplit ds1 = (DataSplit) s1;
          DataSplit ds2 = (DataSplit) s2;

          // Primary sort: snapshot ID for temporal consistency across snapshots
          int snapshotCompare = Long.compare(ds1.snapshotId(), ds2.snapshotId());
          if (snapshotCompare != 0) {
            return snapshotCompare;
          }

          // Secondary sort: min sequence number within same snapshot for LSM ordering
          long seq1 =
              ds1.dataFiles().stream().mapToLong(file -> file.minSequenceNumber()).min().orElse(0L);
          long seq2 =
              ds2.dataFiles().stream().mapToLong(file -> file.minSequenceNumber()).min().orElse(0L);
          return Long.compare(seq1, seq2);
        });

    return sortedSplits;
  }

  /** Extract bucket ID from a Paimon split. */
  private int extractBucketId(Split split) {
    if (split instanceof DataSplit) {
      DataSplit dataSplit = (DataSplit) split;
      // Get bucket from DataSplit's partition information
      return dataSplit.bucket();
    }
    // Default to bucket 0 for non-DataSplit types
    return 0;
  }

  /**
   * Represents a unit of work for a specific bucket. Contains sorted splits for ordered processing
   * within the bucket.
   */
  public static class BucketWork {
    private final int bucketId;
    private final List<Split> splits;
    private final InFlightBatch batch;

    public BucketWork(int bucketId, List<Split> splits, InFlightBatch batch) {
      this.bucketId = bucketId;
      this.splits = splits;
      this.batch = batch;
    }

    public int getBucketId() {
      return bucketId;
    }

    public List<Split> getSplits() {
      return splits;
    }

    public InFlightBatch getBatch() {
      return batch;
    }
  }

  // ============================================================================
  // PROTECTED METHODS FOR TESTING FRAMEWORK
  // ============================================================================

  protected AtomicBoolean getRunning() {
    return running;
  }

  protected BlockingQueue<BucketWork> getWorkQueue() {
    return workQueue;
  }

  protected void setTable(Table table) {
    this.table = table;
  }

  protected void setTableRead(TableRead tableRead) {
    this.tableRead = tableRead;
  }

  protected void setStreamTableScan(StreamTableScan streamTableScan) {
    this.streamTableScan = streamTableScan;
  }

  protected PaimonToAddDocumentConverter getConverter() {
    return converter;
  }

  protected void setConverter(PaimonToAddDocumentConverter converter) {
    this.converter = converter;
  }

  protected void setWorkQueue(BlockingQueue<BucketWork> workQueue) {
    this.workQueue = workQueue;
  }

  /** Log summary statistics for monitoring overall throughput and performance. */
  private void logSummaryStatistics() {
    long totalDocs = totalDocumentsProcessed.get();
    long totalBatches = totalBatchesProcessed.get();
    long totalTimeMs = totalProcessingTimeMs.get();

    if (totalDocs > 0 && totalTimeMs > 0) {
      double overallThroughput = (totalDocs * 1000.0) / totalTimeMs;
      double avgBatchSize = totalBatches > 0 ? (double) totalDocs / totalBatches : 0;
      double avgBatchTime = totalBatches > 0 ? (double) totalTimeMs / totalBatches : 0;

      LOGGER.info(
          "=== PIPELINE SUMMARY === Total: {} docs in {} batches, "
              + "Overall throughput: {} docs/sec, Avg batch: {} docs in {}ms",
          totalDocs,
          totalBatches,
          String.format("%.1f", overallThroughput),
          String.format("%.0f", avgBatchSize),
          String.format("%.1f", avgBatchTime));
    }
  }
}
