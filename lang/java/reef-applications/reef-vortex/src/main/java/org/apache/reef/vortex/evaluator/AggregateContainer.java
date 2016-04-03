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
package org.apache.reef.vortex.evaluator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.vortex.common.KryoUtils;
import org.apache.reef.vortex.protocol.mastertoworker.TaskletAggregation;
import org.apache.reef.vortex.protocol.workertomaster.*;

import javax.annotation.concurrent.GuardedBy;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A container for tasklet aggregation, used to preserve output from individual
 * {@link org.apache.reef.vortex.api.VortexFunction}s and to trigger
 * {@link org.apache.reef.vortex.api.VortexAggregateFunction}s on the pooled outputs.
 */
@Private
@DriverSide
@Unstable
final class AggregateContainer {

  private final Object stateLock = new Object();
  private final TaskletAggregation taskletAggregation;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final KryoUtils kryoUtils;
  private final BlockingDeque<byte[]> workerReportsQueue;
  private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

  @GuardedBy("stateLock")
  private final HashMap<Integer, Integer> pendingTasklets = new HashMap<>();

  @GuardedBy("stateLock")
  private final List<Pair<Integer, Object>> completedTasklets = new ArrayList<>();

  @GuardedBy("stateLock")
  private final List<Pair<Integer, Exception>> failedTasklets = new ArrayList<>();

  AggregateContainer(final HeartBeatTriggerManager heartBeatTriggerManager,
                     final KryoUtils kryoUtils,
                     final BlockingDeque<byte[]> workerReportsQueue,
                     final TaskletAggregation taskletAggregation) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.kryoUtils = kryoUtils;
    this.workerReportsQueue = workerReportsQueue;
    this.taskletAggregation = taskletAggregation;
  }

  public TaskletAggregation getTaskletAggregation() {
    return taskletAggregation;
  }

  @GuardedBy("stateLock")
  private void aggregateTasklets(final List<WorkerToMaster> workerToMasters,
                                 final List<Object> results,
                                 final List<Integer> aggregatedTasklets) {
    synchronized (stateLock) {
      // Add the successful tasklets for aggregation.
      for (final Pair<Integer, Object> resultPair : completedTasklets) {
        aggregatedTasklets.add(resultPair.getLeft());
        results.add(resultPair.getRight());
      }

      // Add failed tasklets to worker report.
      for (final Pair<Integer, Exception> failedPair : failedTasklets) {
        workerToMasters.add(new TaskletFailure(failedPair.getLeft(), failedPair.getRight()));
      }

      // Drain the tasklets.
      completedTasklets.clear();
      failedTasklets.clear();
    }
  }

  /**
   * Performs the output aggregation and generates the {@link WorkerReport} to report back to the
   * {@link org.apache.reef.vortex.driver.VortexDriver}.
   */
  private void aggregateTasklets(final AggregateTriggerType type) {
    final List<WorkerToMaster> workerToMasters = new ArrayList<>();
    final List<Object> results = new ArrayList<>();
    final List<Integer> aggregatedTasklets = new ArrayList<>();

    // Synchronization to prevent duplication of work on the same aggregation function on the same worker.
    synchronized (stateLock) {
      switch(type) {
      case ALARM:
        aggregateTasklets(workerToMasters, results, aggregatedTasklets);
        break;
      case COUNT:
        if (!aggregateOnCount()) {
          return;
        }

        aggregateTasklets(workerToMasters, results, aggregatedTasklets);
        break;
      default:
        throw new RuntimeException("Unexpected aggregate type.");
      }
    }

    if (!results.isEmpty()) {
      // Run the aggregation function.
      try {
        final Object aggregationResult = taskletAggregation.executeAggregation(results);
        workerToMasters.add(new TaskletAggregationResult(aggregatedTasklets, aggregationResult));
      } catch (final Exception e) {
        workerToMasters.add(new TaskletAggregationFailure(aggregatedTasklets, e));
      }
    }

    // Add to worker report only if there is something to report back.
    if (!workerToMasters.isEmpty()) {
      workerReportsQueue.addLast(kryoUtils.serialize(new WorkerReport(workerToMasters)));
      heartBeatTriggerManager.triggerHeartBeat();
    }
  }

  /**
   * Schedule aggregation tasks on a Timer. Creates a new timer schedule for triggering the aggregation function
   * if this is the first time the aggregation function has tasklets scheduled on it.
   * Adds the Tasklet to pending Tasklets.
   */
  public void scheduleTasklet(final int taskletId) {
    synchronized (stateLock) {
      // If there are tasklets are pending to be executed, then that means that a
      // timer has already been scheduled for an aggregation.
      if (!outstandingTasklets()) {
        timer.schedule(new Runnable() {
          @Override
          public void run() {
            aggregateTasklets(AggregateTriggerType.ALARM);
            synchronized (stateLock) {
              // On the callback, if there are tasklets pending to be executed, that means that this alarm
              // was triggered by a previous alarm, so we should continue to trigger more alarms. Otherwise
              // we are done with tasklets for this aggregation function for now.
              // If more tasklets for this aggregation function arrive, it will be triggered by the outer
              // call to timer.schedule.
              if (outstandingTasklets()) {
                timer.schedule(
                    this, taskletAggregation.getPolicy().getPeriodMilliseconds(), TimeUnit.MILLISECONDS);
              }
            }
          }
        }, taskletAggregation.getPolicy().getPeriodMilliseconds(), TimeUnit.MILLISECONDS);
      }

      // Add to pending tasklets, such that on the callback the timer can be refreshed.
      if (!pendingTasklets.containsKey(taskletId)) {
        pendingTasklets.put(taskletId, 0);
      }

      pendingTasklets.put(taskletId, pendingTasklets.get(taskletId) + 1);
    }
  }

  /**
   * Reported when an associated tasklet is complete and adds it to the completion pool.
   */
  public void taskletComplete(final int taskletId, final Object result) {
    final boolean aggregateOnCount;
    synchronized (stateLock) {
      completedTasklets.add(new ImmutablePair<>(taskletId, result));
      removePendingTaskletReferenceCount(taskletId);
      aggregateOnCount = aggregateOnCount();
    }

    if (aggregateOnCount) {
      aggregateTasklets(AggregateTriggerType.COUNT);
    }
  }

  /**
   * Reported when an associated tasklet is complete and adds it to the failure pool.
   */
  public void taskletFailed(final int taskletId, final Exception e) {
    final boolean aggregateOnCount;
    synchronized (stateLock) {
      failedTasklets.add(new ImmutablePair<>(taskletId, e));
      removePendingTaskletReferenceCount(taskletId);
      aggregateOnCount = aggregateOnCount();
    }

    if (aggregateOnCount) {
      aggregateTasklets(AggregateTriggerType.COUNT);
    }
  }

  @GuardedBy("stateLock")
  private void removePendingTaskletReferenceCount(final int taskletId) {
    pendingTasklets.put(taskletId, pendingTasklets.get(taskletId) - 1);
    if (pendingTasklets.get(taskletId) <= 0) {
      pendingTasklets.remove(taskletId);
    }
  }

  @GuardedBy("stateLock")
  private boolean outstandingTasklets() {
    return !(pendingTasklets.isEmpty() && completedTasklets.isEmpty() && failedTasklets.isEmpty());
  }

  @GuardedBy("stateLock")
  private boolean aggregateOnCount() {
    return taskletAggregation.getPolicy().getCount().isPresent() &&
        completedTasklets.size() + failedTasklets.size() >= taskletAggregation.getPolicy().getCount().get();
  }

  private enum AggregateTriggerType {
    ALARM,
    COUNT
  }
}