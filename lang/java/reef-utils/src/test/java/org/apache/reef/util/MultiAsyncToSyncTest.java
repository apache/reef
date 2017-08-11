/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Performs an asynchronous increment of an Integer.
 */
final class AsynchronousIncrementer implements Callable<Integer> {
  private static final Logger LOG = Logger.getLogger(MultiAsyncToSync.class.getName());
  private final int sleepTimeMillis;
  private final int input;
  private final long identifier;
  private final MultiAsyncToSync blocker;

  /**
   * Instantiate an incrementer with specific job parameters.
   * @param input The input parmeter for the work.
   * @param identifier The identifier of the caller to wake on completion.
   * @param sleepTimeMillis How long to work.
   * @param blocker The MultiAsyncToSync object which is holding the blocked client.
   */
  AsynchronousIncrementer(final Integer input, final long identifier,
                                 final int sleepTimeMillis, final MultiAsyncToSync blocker) {
    this.sleepTimeMillis = sleepTimeMillis;
    this.input = input;
    this.identifier = identifier;
    this.blocker = blocker;
  }

  /**
   * Sleep and then ncrement the input value by one.
   * @return The input value of the operation incremented by one.
   * @throws Exception
   */
  public Integer call() throws Exception {
    LOG.log(Level.INFO, "Sleeping...");
    Thread.sleep(sleepTimeMillis);
    LOG.log(Level.INFO, "Releasing caller...");
    blocker.release(identifier);
    return input + 1;
  }
}

/**
 * Use the MultiAyncToSync class to implement a synchronous API
 * that uses asynchronous processing internally.
 */
final class SynchronousApi {
  private static final Logger LOG = Logger.getLogger(SynchronousApi.class.getName());
  private final int incrementerSleepTimeMillis;
  private final MultiAsyncToSync blocker;
  private final ExecutorService executor;
  private final ConcurrentLinkedQueue<FutureTask<Integer>> taskQueue = new ConcurrentLinkedQueue<>();
  private AtomicLong idCounter = new AtomicLong(0);

  /**
   * Parameterize the object as to length of processing time and call timeout.
   * @param incrementerSleepTimeSeconds Length of time the incrementer sleeps before
   *                                    performing the increment and returning.
   * @param timeoutPeriodSeconds The length of time before the call will timeout.
   */
  SynchronousApi(final int incrementerSleepTimeSeconds, final long timeoutPeriodSeconds) {
    this.incrementerSleepTimeMillis = 1000 * incrementerSleepTimeSeconds;
    this.blocker = new MultiAsyncToSync(timeoutPeriodSeconds, SECONDS);
    this.executor = Executors.newFixedThreadPool(2);
  }

  /**
   * The one API call in our interface.
   * @param input
   * @return The input parameter incremented by one or zero for a timeout.
   */
  public Integer apiCall(final Integer input) {
    // Create a future to run the asynchronous processing.
    final long identifier = idCounter.getAndIncrement();
    final FutureTask<Integer> task =
        new FutureTask(new AsynchronousIncrementer(input, identifier, incrementerSleepTimeMillis, blocker));
    taskQueue.add(task);

    LOG.log(Level.INFO, "Running the incrementer...");
    // Start the task and block the caller until it completes.
    executor.execute(task);
    if (blocker.block(identifier)) {
      LOG.log(Level.INFO, "Call timed out...");
      // Timeout occurred before the asynchronous processing completed.
      return 0;
    }
    Integer result = 0;
    try {
      LOG.log(Level.INFO, "Call getting task result...");
      result = task.get();
    } catch (Exception e) {
      result = -1;
    }
    return result;
  }

  /**
   * Insure all test tasks have completed.
   */
  public void complete() {
    try {
      for (FutureTask<Integer> task : taskQueue) {
        task.get();
      }
    } catch (Exception e) {
      LOG.log(Level.INFO, "Caught exception waiting for completion...");
    }
  }
}

/**
 * Verify proper operation of the MultiAsyncToSync class.
 */
public final class MultiAsyncToSyncTest {
  private static final Logger LOG = Logger.getLogger(MultiAsyncToSyncTest.class.getName());

  /**
   * Verify calculations successfully complete when no timeout occurs.
   */
  @Test
  public void testNoTimeout() {
    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 2;
    final long timeoutPeriodSeconds = 4;
    final Integer input = 1;

    SynchronousApi apiObject = new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds);
    final Integer result = apiObject.apiCall(input);
    apiObject.complete();

    Assert.assertTrue(result.equals(input + 1));
  }

  /**
   * Verify an error is returned when a timeout occurs.
   */
  @Test
  public void testTimeout() {
    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 4;
    final long timeoutPeriodSeconds = 2;
    final Integer input = 1;

    SynchronousApi apiObject = new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds);
    final Integer result = apiObject.apiCall(input);
    apiObject.complete();

    Assert.assertTrue(result.equals(0));
  }

  /**
   * Used to make multiple asynchronous calls to the synchronous API.
   */
  final class AsynchronousCaller implements Callable<Boolean> {
    private final SynchronousApi apiObject;
    private final Integer input;

    /**
     * @param apiObject The synchronous api to be calle.
     * @param input Input value for the api call.
     */
    AsynchronousCaller(final SynchronousApi apiObject, final Integer input) {
      this.apiObject = apiObject;
      this.input = input;
    }

    /**
     * @return A Boolean object that indicates whether
     *         or not the processing was successful.
     * @throws Exception
     */
    public Boolean call() throws Exception {
      final Integer result = apiObject.apiCall(input);
      return result.equals(input + 1);
    }
  }

  /**
   * Verify no interaction occurs when multiple calls are in flight.
   */
  @Test
  public void testMulitpleCalls() {
    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 2;
    final long timeoutPeriodSeconds = 4;
    final Integer input = 1;
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    SynchronousApi apiObject = new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds);
    FutureTask<Boolean> task1 = new FutureTask<>(new AsynchronousCaller(apiObject, input));
    FutureTask<Boolean> task2 = new FutureTask<>(new AsynchronousCaller(apiObject, input + 1));

    // Execute API calls concurrently.
    executor.execute(task1);
    executor.execute(task2);

    try {
      Assert.assertTrue(task1.get());
      Assert.assertTrue(task2.get());
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Unexpected exception checking results...");
    }

    apiObject.complete();
  }
}

