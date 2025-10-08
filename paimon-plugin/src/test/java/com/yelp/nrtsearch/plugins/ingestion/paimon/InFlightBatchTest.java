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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Unit tests for InFlightBatch synchronization logic.
 *
 * <p>CRITICAL FAILURE SCENARIOS TESTED: 1. Coordinator deadlock - awaiting completion that never
 * comes 2. Race conditions - multiple workers completing simultaneously 3. Timeout scenarios -
 * workers taking too long to complete 4. Edge cases - zero completions, negative values, interrupt
 * handling
 */
public class InFlightBatchTest {

  @Test
  public void testBasicSynchronization_SingleWorkerCompletion() {
    // SCENARIO: Normal case - single worker completes work, coordinator proceeds
    InFlightBatch batch = new InFlightBatch(12345L, 1);

    assertEquals("Checkpoint ID should match constructor", 12345L, batch.getPaimonCheckpointId());
    assertEquals(
        "Expected completions should match constructor", 1, batch.getExpectedCompletions());
    assertEquals("Should start with full count", 1, batch.getRemainingCount());

    // Simulate worker completing bucket 0
    batch.registerBucket(0);
    batch.markBucketComplete(0);

    assertEquals("Should have zero remaining after completion", 0, batch.getRemainingCount());
  }

  @Test
  public void testMultiWorkerSynchronization_AllWorkersComplete() throws InterruptedException {
    // SCENARIO: Multiple workers completing - coordinator waits for ALL to finish
    final int WORKER_COUNT = 5;
    InFlightBatch batch = new InFlightBatch(67890L, WORKER_COUNT);
    ExecutorService workers = Executors.newFixedThreadPool(WORKER_COUNT);

    CountDownLatch workerStartLatch = new CountDownLatch(WORKER_COUNT);
    AtomicInteger completedWorkers = new AtomicInteger(0);

    // Register all buckets
    for (int i = 0; i < WORKER_COUNT; i++) {
      batch.registerBucket(i);
    }

    // Start multiple workers that complete at different times
    for (int i = 0; i < WORKER_COUNT; i++) {
      final int workerId = i;
      workers.submit(
          () -> {
            try {
              workerStartLatch.countDown();
              // Stagger completion times to test race conditions
              Thread.sleep(workerId * 10);
              batch.markBucketComplete(workerId);
              completedWorkers.incrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    // Ensure all workers have started
    workerStartLatch.await();
    // Note: remaining count may be less than WORKER_COUNT if workers complete quickly

    // Coordinator waits for all workers - this should not hang
    batch.awaitCompletion();

    assertEquals("All workers should have completed", WORKER_COUNT, completedWorkers.get());
    assertEquals("Remaining count should be zero", 0, batch.getRemainingCount());

    workers.shutdown();
  }

  @Test
  public void testTimeoutScenario_WorkersTakeTooLong() throws InterruptedException {
    // SCENARIO: Workers are slow/stuck - coordinator should timeout instead of hanging forever
    InFlightBatch batch = new InFlightBatch(99999L, 2);
    ExecutorService workers = Executors.newFixedThreadPool(2);

    // Register buckets
    batch.registerBucket(0);
    batch.registerBucket(1);

    // Start one fast worker and one slow worker
    workers.submit(
        () -> {
          batch.markBucketComplete(0); // Fast worker completes bucket 0 immediately
        });

    workers.submit(
        () -> {
          try {
            Thread.sleep(5000); // Slow worker takes 5 seconds (too long)
            batch.markBucketComplete(1);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

    // Coordinator times out after 100ms - should not wait forever
    boolean completedInTime = batch.awaitCompletion(100, TimeUnit.MILLISECONDS);

    assertFalse("Should timeout when workers are too slow", completedInTime);
    assertEquals("One worker should still be pending", 1, batch.getRemainingCount());

    workers.shutdown();
  }

  @Test
  public void testRaceCondition_SimultaneousWorkerCompletion() throws InterruptedException {
    // SCENARIO: Many workers completing at exactly the same time - no race conditions
    final int CONCURRENT_WORKERS = 50;
    InFlightBatch batch = new InFlightBatch(11111L, CONCURRENT_WORKERS);
    ExecutorService workers = Executors.newFixedThreadPool(CONCURRENT_WORKERS);

    CountDownLatch simultaneousStart = new CountDownLatch(1);
    AtomicInteger racingCompletions = new AtomicInteger(0);

    // Register all buckets
    for (int i = 0; i < CONCURRENT_WORKERS; i++) {
      batch.registerBucket(i);
    }

    // Create many workers that all complete simultaneously
    for (int i = 0; i < CONCURRENT_WORKERS; i++) {
      final int bucketId = i;
      workers.submit(
          () -> {
            try {
              simultaneousStart.await(); // Wait for signal to start simultaneously
              batch.markBucketComplete(bucketId);
              racingCompletions.incrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    // Release all workers at once to create race condition
    simultaneousStart.countDown();

    // Coordinator should handle the race condition correctly
    batch.awaitCompletion();

    assertEquals(
        "All workers should complete despite race", CONCURRENT_WORKERS, racingCompletions.get());
    assertEquals("Final count should be zero", 0, batch.getRemainingCount());

    workers.shutdown();
  }

  @Test
  public void testEdgeCase_ZeroExpectedCompletions() throws InterruptedException {
    // SCENARIO: Empty batch (no buckets) - coordinator should proceed immediately
    InFlightBatch batch = new InFlightBatch(55555L, 0);

    assertEquals("Should start with zero count", 0, batch.getRemainingCount());

    // Should complete immediately without any worker calling markBucketComplete()
    boolean completed = batch.awaitCompletion(10, TimeUnit.MILLISECONDS);

    assertTrue("Empty batch should complete immediately", completed);
    assertEquals("Count should remain zero", 0, batch.getRemainingCount());
  }

  @Test
  public void testInterruptScenario_CoordinatorThreadInterrupted() throws InterruptedException {
    // SCENARIO: Coordinator thread gets interrupted while waiting - should handle gracefully
    InFlightBatch batch = new InFlightBatch(77777L, 1);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    Thread coordinatorThread =
        new Thread(
            () -> {
              try {
                batch.awaitCompletion(); // This will block since no worker completes
              } catch (InterruptedException e) {
                wasInterrupted.set(true);
                Thread.currentThread().interrupt(); // Preserve interrupt status
              }
            });

    coordinatorThread.start();
    Thread.sleep(50); // Let coordinator start waiting

    // Interrupt the coordinator while it's waiting
    coordinatorThread.interrupt();

    coordinatorThread.join(1000); // Wait for thread to finish

    assertTrue("Coordinator should have been interrupted", wasInterrupted.get());
    assertTrue(
        "Coordinator thread should preserve interrupt status", coordinatorThread.isInterrupted());
    assertEquals("Work should still be pending", 1, batch.getRemainingCount());
  }

  @Test
  public void testFailureScenario_SomeWorkersNeverComplete() throws InterruptedException {
    // SCENARIO: Some workers fail/crash without calling markBucketComplete() - coordinator waits
    // forever
    // This simulates the "safe halt" behavior mentioned in PaimonIngestor
    InFlightBatch batch = new InFlightBatch(88888L, 3);

    // Register 3 buckets
    batch.registerBucket(0);
    batch.registerBucket(1);
    batch.registerBucket(2);

    // Two workers complete successfully
    batch.markBucketComplete(0);
    batch.markBucketComplete(1);

    assertEquals("One worker should still be pending", 1, batch.getRemainingCount());

    // Third worker (bucket 2) crashes/fails - never calls markBucketComplete()
    // Coordinator should timeout (not wait forever)
    boolean completed = batch.awaitCompletion(100, TimeUnit.MILLISECONDS);

    assertFalse("Should not complete when worker fails", completed);
    assertEquals("Failed worker should keep count at 1", 1, batch.getRemainingCount());
  }

  @Test
  public void testDoubleCompletion_ExtraMarkBucketCompleteCall() {
    // SCENARIO: Worker accidentally calls markBucketComplete() twice - should handle gracefully
    InFlightBatch batch = new InFlightBatch(22222L, 2);

    // Register 2 buckets
    batch.registerBucket(0);
    batch.registerBucket(1);

    // First worker completes normally
    batch.markBucketComplete(0);
    assertEquals("Should have one remaining", 1, batch.getRemainingCount());

    // Second worker completes normally
    batch.markBucketComplete(1);
    assertEquals("Should have zero remaining", 0, batch.getRemainingCount());

    // Buggy worker calls markBucketComplete() again for bucket 0 - should not go negative
    batch.markBucketComplete(0);
    assertEquals("Count should not go below zero", 0, batch.getRemainingCount());
  }

  @Test
  public void testLargeScale_ManyExpectedCompletions() throws InterruptedException {
    // SCENARIO: Large number of buckets - ensure no integer overflow or performance issues
    final int LARGE_BUCKET_COUNT = 10000;
    InFlightBatch batch = new InFlightBatch(44444L, LARGE_BUCKET_COUNT);

    assertEquals("Should handle large counts", LARGE_BUCKET_COUNT, batch.getRemainingCount());

    // Register and complete all work
    for (int i = 0; i < LARGE_BUCKET_COUNT; i++) {
      batch.registerBucket(i);
    }
    for (int i = 0; i < LARGE_BUCKET_COUNT; i++) {
      batch.markBucketComplete(i);
    }

    assertEquals("Should handle large completion counts", 0, batch.getRemainingCount());

    // Should complete immediately
    boolean completed = batch.awaitCompletion(10, TimeUnit.MILLISECONDS);
    assertTrue("Large batch should complete when all work done", completed);
  }
}
