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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.vortex.common.VortexFutureDelegate;
import org.apache.reef.wake.remote.Codec;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * The interface between user code and aggregation Tasklets.
 * Thread safety: This class is not meant to be used in a multi-threaded fashion.
 * TODO[JIRA REEF-1131]: Create and run tests once functional.
 */
@Public
@ClientSide
@NotThreadSafe
@Unstable
public final class VortexAggregateFuture<TInput, TOutput> implements VortexFutureDelegate {
  private static final Logger LOG = Logger.getLogger(VortexAggregateFuture.class.getName());

  private final Executor executor;
  private final Codec<TOutput> aggOutputCodec;
  private final BlockingQueue<AggregateResult> resultQueue;
  private final Map<Integer, Pair<TInput, VortexFunction<TInput, TOutput>>> taskletIdFunctionMap;
  private final FutureCallback<AggregateResult<TInput, TOutput>> callbackHandler;

  @Private
  public VortexAggregateFuture(final Executor executor,
                               final Map<Integer, Pair<TInput, VortexFunction<TInput, TOutput>>> taskletIdFunctionMap,
                               final Codec<TOutput> aggOutputCodec,
                               final FutureCallback<AggregateResult<TInput, TOutput>> callbackHandler) {
    this.executor = executor;
    this.taskletIdFunctionMap = new HashMap<>(taskletIdFunctionMap);
    this.resultQueue = new ArrayBlockingQueue<>(taskletIdFunctionMap.size());
    this.aggOutputCodec = aggOutputCodec;
    this.callbackHandler = callbackHandler;
  }

  /**
   * @return the next aggregation result for the future, null if no more results.
   */
  public synchronized AggregateResult get() throws InterruptedException {
    if (taskletIdFunctionMap.isEmpty()) {
      return null;
    }

    return resultQueue.take();
  }

  /**
   * @param timeout the timeout for the operation.
   * @param timeUnit the time unit of the timeout.
   * @return the next aggregation result for the future, within the user specified timeout, null if no more results.
   * @throws TimeoutException if time out hits.
   */
  public synchronized AggregateResult get(final long timeout,
                                          final TimeUnit timeUnit) throws InterruptedException, TimeoutException {
    if (taskletIdFunctionMap.isEmpty()) {
      return null;
    }

    final AggregateResult result = resultQueue.poll(timeout, timeUnit);

    if (result == null) {
      throw new TimeoutException();
    }

    return result;
  }

  /**
   * @return true if there are no more results to poll.
   */
  public synchronized boolean isDone() {
    return taskletIdFunctionMap.size() == 0;
  }

  /**
   * A Tasklet associated with the aggregation has completed.
   */
  @Private
  @Override
  public void completed(final int taskletId, final byte[] serializedResult) {
    try {
      // TODO[REEF-1113]: Handle serialization failure separately in Vortex
      final TOutput result = aggOutputCodec.decode(serializedResult);
      removeCompletedTasklets(result, Collections.singletonList(taskletId));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Aggregation has completed for a list of Tasklets, with an aggregated result.
   */
  @Private
  @Override
  public void aggregationCompleted(final List<Integer> pTaskletIds, final byte[] serializedResult) {
    try {
      // TODO[REEF-1113]: Handle serialization failure separately in Vortex
      final TOutput result = aggOutputCodec.decode(serializedResult);
      removeCompletedTasklets(result, pTaskletIds);
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
      removeFailedTasklets(exception, Collections.singletonList(taskletId));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A list of Tasklets has failed during aggregation phase.
   */
  @Private
  @Override
  public void aggregationThrewException(final List<Integer> pTaskletIds, final Exception exception) {
    try {
      removeFailedTasklets(exception, pTaskletIds);
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
   * Removes completed Tasklets from Tasklets that are expected and invoke callback.
   */
  private synchronized void removeCompletedTasklets(final TOutput output, final List<Integer> taskletIds)
      throws InterruptedException {
    final AggregateResult result =
        new AggregateResult(output, getInputFunctions(taskletIds), taskletIdFunctionMap.size() > 0);

    if (callbackHandler != null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callbackHandler.onSuccess(result);
        }
      });
    }

    resultQueue.put(result);
  }

  /**
   * Removes failed Tasklets from Tasklets that are expected and invokes callback.
   */
  private synchronized void removeFailedTasklets(final Exception exception, final List<Integer> taskletIds)
      throws InterruptedException {

    final AggregateResult result =
        new AggregateResult(exception, getInputFunctions(taskletIds), taskletIdFunctionMap.size() > 0);

    if (callbackHandler != null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          // Note that success is still called here
          // User should be checking the Exception status in AggregateResult.
          // TODO[JIRA REEF-1129]: Add documentation in VortexThreadPool.
          callbackHandler.onSuccess(result);
        }
      });
    }

    resultQueue.put(result);
  }

  /**
   * Gets the inputs and their respective {@link VortexFunction}s on Tasklet aggregation completion.
   */
  private synchronized List<Pair<TInput, VortexFunction<TInput, TOutput>>> getInputFunctions(
      final List<Integer> taskletIds) {

    final List<Pair<TInput, VortexFunction<TInput, TOutput>>> inputList = new ArrayList<>(taskletIds.size());

    for(final int taskletId : taskletIds) {
      inputList.add(taskletIdFunctionMap.remove(taskletId));
    }

    return inputList;
  }
}