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
package org.apache.reef.vortex.api;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.vortex.driver.VortexFutureDelegate;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.*;

/**
 * The interface between user code and aggregation Tasklets.
 * Thread safety: This class is not meant to be used in a multi-threaded fashion.
 * TODO[JIRA REEF-1131]: Create and run tests once functional.
 */
@Public
@ClientSide
@NotThreadSafe
@Unstable
public final class VortexAggregateFuture<TInput, TOutput> implements VortexFutureDelegate<TOutput> {
  private final Executor executor;
  private final BlockingQueue<Pair<List<Integer>, AggregateResult>> resultQueue;
  private final ConcurrentMap<Integer, TInput> taskletIdInputMap;
  private final FutureCallback<AggregateResult<TInput, TOutput>> callbackHandler;

  @Private
  public VortexAggregateFuture(final Executor executor,
                               final Map<Integer, TInput> taskletIdInputMap,
                               final FutureCallback<AggregateResult<TInput, TOutput>> callbackHandler) {
    this.executor = executor;
    this.taskletIdInputMap = new ConcurrentHashMap<>(taskletIdInputMap);
    this.resultQueue = new ArrayBlockingQueue<>(taskletIdInputMap.size());
    this.callbackHandler = callbackHandler;
  }

  /**
   * @return the next aggregation result for the future, null if no more results.
   */
  public synchronized AggregateResultSynchronous<TInput, TOutput> get() throws InterruptedException {
    if (taskletIdInputMap.isEmpty()) {
      return null;
    }

    final Pair<List<Integer>, AggregateResult> resultPair = resultQueue.take();

    removeFromTaskletIdInputMap(resultPair.getLeft());
    return new AggregateResultSynchronous<>(resultPair.getRight(), !taskletIdInputMap.isEmpty());
  }

  /**
   * @param timeout the timeout for the operation.
   * @param timeUnit the time unit of the timeout.
   * @return the next aggregation result for the future, within the user specified timeout, null if no more results.
   * @throws TimeoutException if time out hits.
   */
  public synchronized AggregateResultSynchronous<TInput, TOutput> get(final long timeout, final TimeUnit timeUnit)
      throws InterruptedException, TimeoutException {
    if (taskletIdInputMap.isEmpty()) {
      return null;
    }

    final Pair<List<Integer>, AggregateResult> resultPair = resultQueue.poll(timeout, timeUnit);

    if (resultPair == null) {
      throw new TimeoutException("Synchronous aggregation of the next future result timed out. Timeout = " + timeout
              + " in time units: " + timeUnit);
    }

    removeFromTaskletIdInputMap(resultPair.getLeft());
    return new AggregateResultSynchronous<>(resultPair.getRight(), !taskletIdInputMap.isEmpty());
  }

  private void removeFromTaskletIdInputMap(final List<Integer> taskletIds) {
    for (final int taskletId : taskletIds) {
      taskletIdInputMap.remove(taskletId);
    }
  }

  /**
   * @return true if there are no more results to poll.
   */
  public boolean isDone() {
    return taskletIdInputMap.isEmpty();
  }

  /**
   * A Tasklet associated with the aggregation has completed.
   */
  @Private
  @Override
  public void completed(final int taskletId, final TOutput result) {
    try {
      completedTasklets(result, Collections.singletonList(taskletId));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Aggregation has completed for a list of Tasklets, with an aggregated result.
   */
  @Private
  @Override
  public void aggregationCompleted(final List<Integer> taskletIds, final TOutput result) {
    try {
      completedTasklets(result, taskletIds);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A Tasklet associated with the aggregation has failed.
   */
  @Private
  @Override
  public void threwException(final int taskletId, final Exception exception) {
    try {
      failedTasklets(exception, Collections.singletonList(taskletId));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A list of Tasklets has failed during aggregation phase.
   */
  @Private
  @Override
  public void aggregationThrewException(final List<Integer> taskletIds, final Exception exception) {
    try {
      failedTasklets(exception, taskletIds);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Not implemented for local aggregation.
   */
  @Private
  @Override
  public void cancelled(final int taskletId) {
    throw new NotImplementedException("Tasklet cancellation not supported in aggregations.");
  }

  /**
   * Create and queue result for Tasklets that are expected and invoke callback.
   */
  private void completedTasklets(final TOutput output, final List<Integer> taskletIds)
      throws InterruptedException {
    final List<TInput> inputs = getInputs(taskletIds);
    final AggregateResult result = new AggregateResult(output, inputs);

    if (callbackHandler != null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callbackHandler.onSuccess(result);
        }
      });
    }

    resultQueue.put(new ImmutablePair<>(taskletIds, result));
  }

  /**
   * Create and queue result for failed Tasklets that are expected and invokes callback.
   */
  private void failedTasklets(final Exception exception, final List<Integer> taskletIds)
      throws InterruptedException {

    final List<TInput> inputs = getInputs(taskletIds);
    final AggregateResult failure = new AggregateResult(exception, inputs);

    if (callbackHandler != null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callbackHandler.onFailure(new VortexAggregateException(exception, inputs));
        }
      });
    }

    resultQueue.put(new ImmutablePair<>(taskletIds, failure));
  }

  /**
   * Gets the inputs on Tasklet aggregation completion.
   */
  private List<TInput> getInputs(final List<Integer> taskletIds) {

    final List<TInput> inputList = new ArrayList<>(taskletIds.size());

    for(final int taskletId : taskletIds) {
      inputList.add(taskletIdInputMap.get(taskletId));
    }

    return inputList;
  }
}