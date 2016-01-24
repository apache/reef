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
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.*;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;

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
  private final TaskletAggregationRequest taskletAggregationRequest;

  @GuardedBy("stateLock")
  private final List<Pair<Integer, Object>> completedTasklets = new ArrayList<>();

  @GuardedBy("stateLock")
  private final List<Pair<Integer, Exception>> failedTasklets = new ArrayList<>();

  AggregateContainer(final TaskletAggregationRequest taskletAggregationRequest) {
    this.taskletAggregationRequest = taskletAggregationRequest;
  }

  public TaskletAggregationRequest getTaskletAggregationRequest() {
    return taskletAggregationRequest;
  }

  /**
   * Performs the output aggregation and generates the {@link WorkerReport} to report back to the
   * {@link org.apache.reef.vortex.driver.VortexDriver}.
   */
  public Optional<WorkerReport> aggregateTasklets() {
    final List<TaskletReport> taskletReports = new ArrayList<>();
    final List<Object> results = new ArrayList<>();
    final List<Integer> aggregatedTasklets = new ArrayList<>();

    // Synchronization to prevent duplication of work on the same aggregation function on the same worker.
    synchronized (stateLock) {
      // Add the successful tasklets for aggregation.
      for (final Pair<Integer, Object> resultPair : completedTasklets) {
        aggregatedTasklets.add(resultPair.getLeft());
        results.add(resultPair.getRight());
      }

      // Add failed tasklets to worker report.
      for (final Pair<Integer, Exception> failedPair : failedTasklets) {
        taskletReports.add(new TaskletFailureReport(failedPair.getLeft(), failedPair.getRight()));
      }

      // Drain the tasklets.
      completedTasklets.clear();
      failedTasklets.clear();
    }

    if (!results.isEmpty()) {
      // Run the aggregation function.
      try {
        final byte[] aggregationResult = taskletAggregationRequest.executeAggregation(results);
        taskletReports.add(new TaskletAggregationResultReport(aggregatedTasklets, aggregationResult));
      } catch (final Exception e) {
        taskletReports.add(new TaskletAggregationFailureReport(aggregatedTasklets, e));
      }
    }

    return taskletReports.isEmpty() ? Optional.<WorkerReport>empty() : Optional.of(new WorkerReport(taskletReports));
  }

  /**
   * Reported when an associated tasklet is complete and adds it to the completion pool.
   */
  public void taskletComplete(final int taskletId, final Object result) {
    synchronized (stateLock) {
      completedTasklets.add(new ImmutablePair<>(taskletId, result));
    }
  }

  /**
   * Reported when an associated tasklet is complete and adds it to the failure pool.
   */
  public void taskletFailed(final int taskletId, final Exception e) {
    synchronized (stateLock) {
      failedTasklets.add(new ImmutablePair<>(taskletId, e));
    }
  }
}