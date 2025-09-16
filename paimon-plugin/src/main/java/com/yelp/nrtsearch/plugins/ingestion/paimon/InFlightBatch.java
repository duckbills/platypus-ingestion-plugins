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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tracks a batch of work dispatched by the coordinator. Provides synchronization to ensure
 * checkpoint only happens after all work is complete.
 */
public class InFlightBatch {
  private final long paimonCheckpointId;
  private final CountDownLatch latch;
  private final int expectedCompletions;

  public InFlightBatch(long paimonCheckpointId, int expectedCompletions) {
    this.paimonCheckpointId = paimonCheckpointId;
    this.expectedCompletions = expectedCompletions;
    this.latch = new CountDownLatch(expectedCompletions);
  }

  public long getPaimonCheckpointId() {
    return paimonCheckpointId;
  }

  public int getExpectedCompletions() {
    return expectedCompletions;
  }

  /**
   * Wait for all buckets in this batch to complete processing.
   *
   * @throws InterruptedException if interrupted while waiting
   */
  public void awaitCompletion() throws InterruptedException {
    this.latch.await();
  }

  /**
   * Wait for all buckets in this batch to complete processing with timeout.
   *
   * @param timeout timeout duration
   * @param unit timeout unit
   * @return true if completed within timeout, false if timed out
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
    return this.latch.await(timeout, unit);
  }

  /**
   * Signal that one bucket has completed processing successfully. Called by worker threads after
   * successful commit.
   */
  public void markBucketComplete() {
    this.latch.countDown();
  }

  /**
   * Get the current count of remaining completions needed.
   *
   * @return number of buckets still processing
   */
  public long getRemainingCount() {
    return this.latch.getCount();
  }
}
