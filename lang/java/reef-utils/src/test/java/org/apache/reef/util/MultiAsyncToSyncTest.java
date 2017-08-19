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

import org.apache.reef.util.exception.InvalidIdentifierException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Performs an asynchronous increment of an Integer.
 */
final class AsynchronousIncrementer implements Callable<Integer> {
  private static final Logger LOG = Logger.getLogger(AsynchronousIncrementer.class.getName());
  private final int sleepTimeMillis;
  private final int input;
  private final long identifier;
  private final MultiAsyncToSync blocker;

  /**
   * Instantiate an incrementer with specific job parameters.
   * @param input The input parameter for the work.
   * @param identifier The identifier of the caller to wake on completion.
   * @param sleepTimeMillis How long to work.
   * @param blocker The MultiAsyncToSync object which is holding the blocked client.
   */
  AsynchronousIncrementer(final int input, final long identifier,
                                 final int sleepTimeMillis, final MultiAsyncToSync blocker) {
    this.sleepTimeMillis = sleepTimeMillis;
    this.input = input;
    this.identifier = identifier;
    this.blocker = blocker;
  }

  /**
   * Sleep and then increment the input value by one.
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
 * Use the MultiAsyncToSync class to implement a synchronous API
 * that uses asynchronous processing internally.
 */
final class SynchronousApi implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(SynchronousApi.class.getName());
  private final int incrementerSleepTimeMillis;
  private final MultiAsyncToSync blocker;
  private final ExecutorService executor;
  private final ConcurrentLinkedQueue<FutureTask<Integer>> taskQueue = new ConcurrentLinkedQueue<>();
  private final AtomicLong idCounter = new AtomicLong(0);
  private final boolean expectTimeout;

  /**
   * Parameterize the object as to length of processing time and call timeout.
   * @param incrementerSleepTimeSeconds Length of time the incrementer sleeps before
   *                                    performing the increment and returning.
   * @param timeoutPeriodSeconds The length of time before the call will timeout.
   */
  SynchronousApi(final int incrementerSleepTimeSeconds,
                 final long timeoutPeriodSeconds, final boolean expectTimeout) {
    this.expectTimeout = expectTimeout;
    this.incrementerSleepTimeMillis = 1000 * incrementerSleepTimeSeconds;
    this.blocker = new MultiAsyncToSync(timeoutPeriodSeconds, SECONDS);
    this.executor = Executors.newFixedThreadPool(2);
  }

  private class AsyncInitiator implements Callable<Boolean> {
    private final FutureTask<Integer> task;
    private final ExecutorService executor;

    AsyncInitiator(final FutureTask<Integer> task, final ExecutorService executor) {
      this.task = task;
      this.executor = executor;
    }

    public Boolean call() {
      executor.execute(task);
      return true;
    }
  }

  /**
   * Asynchronously increment the input parameter.
   * @param input An integer object whose value is to be incremented by one.
   * @return The input parameter incremented by one or zero for a timeout.
   * @throws InterruptedException Thread was interrupted by another thread.
   * @throws ExecutionException An exception was thrown an internal processing function.
   * @throws InvalidIdentifierException The call identifier is invalid.
   * @throws Exception The asynchronous processing generated an exception.
   */
  public int apiCall(final Integer input) throws Exception {
    // Create a future to run the asynchronous processing.
    final long identifier = idCounter.getAndIncrement();
    final FutureTask<Integer> task =
        new FutureTask<>(new AsynchronousIncrementer(input, identifier, incrementerSleepTimeMillis, blocker));
    taskQueue.add(task);

    LOG.log(Level.INFO, "Running the incrementer...");
    if (blocker.block(identifier, new AsyncInitiator(task, executor))) {
      LOG.log(Level.INFO, "Call timed out...");
      // Timeout occurred before the asynchronous processing completed.
      return 0;
    }
    int result = -1;
    LOG.log(Level.INFO, "Call getting task result...");
    return task.get();
  }

  /**
   * Ensure all test tasks have completed.
   * @throws ExecutionException Asynchronous processing generated an exception.
   */
  public void close() throws ExecutionException {
    for (final FutureTask<Integer> task : taskQueue) {
      try {
        task.get();
      } catch (final ExecutionException ee) {
        // When asynchronous processing completes after the call to MultiAsyncToSync.block() times
        // out, the call to MultiAsyncToSync.release() will throw this an InvalidIdentifierException
        // and it is expected; otherwise, we flag the error.
        if (!(expectTimeout && ee.getCause() instanceof InvalidIdentifierException)) {
          LOG.log(Level.INFO, "Caught exception waiting for completion...", ee);
        }
      } catch (final Exception e) {
        LOG.log(Level.INFO, "Caught exception waiting for completion...", e);
      }
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
  public void testNoTimeout() throws Exception {
    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 2;
    final long timeoutPeriodSeconds = 4;
    final int input = 1;

    try (final SynchronousApi apiObject =
           new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds, false)) {
      final int result = apiObject.apiCall(input);
      Assert.assertEquals("Value incremented by one", input + 1, result);
    }
  }

  /**
   * Verify an error is returned when a timeout occurs.
   */
  @Test
  public void testTimeout() throws Exception {
    LOG.log(Level.INFO, "Starting...");

    // Parameters that force a timeout.
    final int incrementerSleepTimeSeconds = 4;
    final long timeoutPeriodSeconds = 2;
    final int input = 1;

    try (final SynchronousApi apiObject =
           new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds, true)) {
      final int result = apiObject.apiCall(input);
      Assert.assertEquals("Timeout occurred", result, 0);
    }
  }

  /**
   * Verify no interaction occurs when multiple calls are in flight.
   */
  @Test
  public void testMultipleCalls() throws Exception {

    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 2;
    final long timeoutPeriodSeconds = 4;

    try (final SynchronousApi apiObject =
           new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds, false)) {
      final String function = "apiCall";
      final int input = 1;
      final FutureTask<Integer> task1 =
          new FutureTask<>(new MethodCallable<Integer>(apiObject, function, input));
      final FutureTask<Integer> task2
          = new FutureTask<>(new MethodCallable<Integer>(apiObject, function, input + 1));

      // Execute API calls concurrently.
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      executor.execute(task1);
      executor.execute(task2);

      final int result1 = task1.get();
      final int result2 = task2.get();

      Assert.assertEquals("Input must be incremented by one", input + 1, result1);
      Assert.assertEquals("Input must be incremented by one", input + 2, result2);
    }
  }

  /**
   * Verify no race conditions occurs when multiple calls are in flight.
   */
  @Test
  public void testRaceConditions() throws Exception {

    LOG.log(Level.INFO, "Starting...");

    // Parameters that do not force a timeout.
    final int incrementerSleepTimeSeconds = 1;
    final long timeoutPeriodSeconds = 10;
    final String function = "apiCall";

    final int nTasks = 100;
    final FutureTask[] tasks = new FutureTask[nTasks];
    final ExecutorService executor = Executors.newFixedThreadPool(10);

    try (final SynchronousApi apiObject =
           new SynchronousApi(incrementerSleepTimeSeconds, timeoutPeriodSeconds, false)) {

      for (int idx = 0; idx < nTasks; ++idx) {
        tasks[idx] = new FutureTask<>(new MethodCallable<Integer>(apiObject, function, idx));
        executor.execute(tasks[idx]);
      }

      for (int idx = 0; idx < nTasks; ++idx) {
        final int result = (int)tasks[idx].get();
        Assert.assertEquals("Input must be incremented by one", idx + 1, result);
      }
    }
  }
}

