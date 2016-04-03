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
package org.apache.reef.vortex.driver;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.*;
import org.apache.reef.vortex.protocol.workertomaster.*;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of VortexMaster.
 * Uses two thread-safe data structures(pendingTasklets, runningWorkers) in implementing VortexMaster interface.
 */
@ThreadSafe
@DriverSide
final class DefaultVortexMaster implements VortexMaster {
  private final Map<Integer, VortexFutureDelegate> taskletFutureMap = new HashMap<>();
  private final AtomicInteger taskletIdCounter = new AtomicInteger();
  private final AtomicInteger aggregateIdCounter = new AtomicInteger();
  private final AggregateFunctionRepository aggregateFunctionRepository;
  private final RunningWorkers runningWorkers;
  private final PendingTasklets pendingTasklets;
  private final Executor executor;

  /**
   * @param runningWorkers for managing all running workers.
   */
  @Inject
  DefaultVortexMaster(final RunningWorkers runningWorkers,
                      final PendingTasklets pendingTasklets,
                      final AggregateFunctionRepository aggregateFunctionRepository,
                      @Parameter(VortexMasterConf.CallbackThreadPoolSize.class) final int threadPoolSize) {
    this.executor = Executors.newFixedThreadPool(threadPoolSize);
    this.runningWorkers = runningWorkers;
    this.pendingTasklets = pendingTasklets;
    this.aggregateFunctionRepository = aggregateFunctionRepository;
  }

  /**
   * Add a new tasklet to pendingTasklets.
   */
  @Override
  public <TInput, TOutput> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> function, final TInput input,
                     final Optional<FutureCallback<TOutput>> callback) {
    // TODO[REEF-500]: Simple duplicate Vortex Tasklet launch.
    final VortexFuture<TOutput> vortexFuture;
    final int id = taskletIdCounter.getAndIncrement();
    if (callback.isPresent()) {
      vortexFuture = new VortexFuture<>(executor, this, id, callback.get());
    } else {
      vortexFuture = new VortexFuture<>(executor, this, id);
    }

    final Tasklet tasklet = new Tasklet<>(id, Optional.<Integer>empty(), function, input, vortexFuture);
    putDelegate(Collections.singletonList(tasklet), vortexFuture);
    this.pendingTasklets.addLast(tasklet);

    return vortexFuture;
  }

  /**
   * Add aggregate-able Tasklets to pendingTasklets.
   */
  @Override
  public <TInput, TOutput> VortexAggregateFuture<TInput, TOutput>
      enqueueTasklets(final VortexAggregateFunction<TOutput> aggregateFunction,
                      final VortexFunction<TInput, TOutput> vortexFunction,
                      final VortexAggregatePolicy policy,
                      final List<TInput> inputs,
                      final Optional<FutureCallback<AggregateResult<TInput, TOutput>>> callback) {
    final int aggregateFunctionId = aggregateIdCounter.getAndIncrement();
    aggregateFunctionRepository.put(aggregateFunctionId, aggregateFunction, policy);
    final List<Tasklet> tasklets = new ArrayList<>(inputs.size());
    final Map<Integer, TInput> taskletIdInputMap = new HashMap<>(inputs.size());

    for (final TInput input : inputs) {
      taskletIdInputMap.put(taskletIdCounter.getAndIncrement(), input);
    }

    final VortexAggregateFuture<TInput, TOutput> vortexAggregateFuture;
    if (callback.isPresent()) {
      vortexAggregateFuture = new VortexAggregateFuture<>(executor, taskletIdInputMap, callback.get());
    } else {
      vortexAggregateFuture = new VortexAggregateFuture<>(executor, taskletIdInputMap, null);
    }

    for (final Map.Entry<Integer, TInput> taskletIdInputEntry : taskletIdInputMap.entrySet()) {
      final Tasklet tasklet = new Tasklet<>(taskletIdInputEntry.getKey(), Optional.of(aggregateFunctionId),
          vortexFunction, taskletIdInputEntry.getValue(), vortexAggregateFuture);
      tasklets.add(tasklet);
      pendingTasklets.addLast(tasklet);
    }

    putDelegate(tasklets, vortexAggregateFuture);
    return vortexAggregateFuture;
  }

  /**
   * Cancels tasklets on the running workers.
   */
  @Override
  public void cancelTasklet(final boolean mayInterruptIfRunning, final int taskletId) {
    this.runningWorkers.cancelTasklet(mayInterruptIfRunning, taskletId);
  }

  /**
   * Add a new worker to runningWorkers.
   */
  @Override
  public void workerAllocated(final VortexWorkerManager vortexWorkerManager) {
    runningWorkers.addWorker(vortexWorkerManager);
  }

  /**
   * Remove the worker from runningWorkers and add back the lost tasklets to pendingTasklets.
   */
  @Override
  public void workerPreempted(final String id) {
    final Optional<Collection<Tasklet>> preemptedTasklets = runningWorkers.removeWorker(id);
    if (preemptedTasklets.isPresent()) {
      for (final Tasklet tasklet : preemptedTasklets.get()) {
        pendingTasklets.addFirst(tasklet);
      }
    }
  }

  @Override
  public void workerReported(final String workerId, final WorkerToMasterReports workerToMasterReports) {
    for (final WorkerToMasterReport workerToMasterReport : workerToMasterReports.getReports()) {
      switch (workerToMasterReport.getType()) {
      case TaskletResult:
        final TaskletResultReport taskletResultReport = (TaskletResultReport) workerToMasterReport;

        final int resultTaskletId = taskletResultReport.getTaskletId();
        final List<Integer> singletonResultTaskletId = Collections.singletonList(resultTaskletId);
        runningWorkers.doneTasklets(workerId, singletonResultTaskletId);
        fetchDelegate(singletonResultTaskletId).completed(resultTaskletId, taskletResultReport.getResult());

        break;
      case TaskletAggregationResult:
        final TaskletAggregationResultReport taskletAggregationResultReport =
            (TaskletAggregationResultReport) workerToMasterReport;

        final List<Integer> aggregatedTaskletIds = taskletAggregationResultReport.getTaskletIds();
        runningWorkers.doneTasklets(workerId, aggregatedTaskletIds);
        fetchDelegate(aggregatedTaskletIds).aggregationCompleted(
            aggregatedTaskletIds, taskletAggregationResultReport.getResult());

        break;
      case TaskletCancelled:
        final TaskletCancelledReport taskletCancelledReport = (TaskletCancelledReport) workerToMasterReport;
        final List<Integer> cancelledIdToList = Collections.singletonList(taskletCancelledReport.getTaskletId());
        runningWorkers.doneTasklets(workerId, cancelledIdToList);
        fetchDelegate(cancelledIdToList).cancelled(taskletCancelledReport.getTaskletId());

        break;
      case TaskletFailure:
        final TaskletFailureReport taskletFailureReport = (TaskletFailureReport) workerToMasterReport;

        final int failureTaskletId = taskletFailureReport.getTaskletId();
        final List<Integer> singletonFailedTaskletId = Collections.singletonList(failureTaskletId);
        runningWorkers.doneTasklets(workerId, singletonFailedTaskletId);
        fetchDelegate(singletonFailedTaskletId).threwException(failureTaskletId, taskletFailureReport.getException());

        break;
      case TaskletAggregationFailure:
        final TaskletAggregationFailureReport taskletAggregationFailureReport =
            (TaskletAggregationFailureReport) workerToMasterReport;

        final List<Integer> aggregationFailedTaskletIds = taskletAggregationFailureReport.getTaskletIds();
        runningWorkers.doneTasklets(workerId, aggregationFailedTaskletIds);
        fetchDelegate(aggregationFailedTaskletIds).aggregationThrewException(aggregationFailedTaskletIds,
            taskletAggregationFailureReport.getException());
        break;
      default:
        throw new RuntimeException("Unknown Report");
      }
    }
  }

  /**
   * Terminate the job.
   */
  @Override
  public void terminate() {
    runningWorkers.terminate();
  }

  /**
   * Puts a delegate to associate with a Tasklet.
   */
  private synchronized void putDelegate(final List<Tasklet> tasklets, final VortexFutureDelegate delegate) {
    for (final Tasklet tasklet : tasklets) {
      taskletFutureMap.put(tasklet.getId(), delegate);
    }
  }

  /**
   * Fetches a delegate that maps to the list of Tasklets.
   */
  private synchronized VortexFutureDelegate fetchDelegate(final List<Integer> taskletIds) {
    VortexFutureDelegate delegate = null;
    for (final int taskletId : taskletIds) {
      final VortexFutureDelegate currDelegate = taskletFutureMap.remove(taskletId);
      if (currDelegate == null) {
        // TODO[JIRA REEF-500]: Consider duplicate tasklets.
        throw new RuntimeException("Tasklet should only be removed once.");
      }

      if (delegate == null) {
        delegate = currDelegate;
      } else {
        assert delegate == currDelegate;
      }
    }

    return delegate;
  }

}
