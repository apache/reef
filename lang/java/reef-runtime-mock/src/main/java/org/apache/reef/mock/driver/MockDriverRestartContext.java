/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.mock.driver;

import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.mock.driver.runtime.MockActiveContext;
import org.apache.reef.mock.driver.runtime.MockAllocatedEvalautor;
import org.apache.reef.mock.driver.runtime.MockFailedEvaluator;
import org.apache.reef.mock.driver.runtime.MockRunningTask;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import java.util.*;

/**
 * Contains the runtime driver state at the time of a driver
 * failure, triggered by {@link MockFailure}.
 */
public final class MockDriverRestartContext {

  private final int restartAttemps;

  private final StartTime startTime;

  private final List<MockAllocatedEvalautor> allocatedEvalautors;

  private final List<MockActiveContext> activeContexts;

  private final List<MockRunningTask> runningTasks;

  private final List<MockFailedEvaluator> failedEvaluators;

  public MockDriverRestartContext(
      final int restartAttemps,
      final StartTime startTime,
      final List<MockAllocatedEvalautor> allocatedEvalautors,
      final List<MockActiveContext> activeContexts,
      final List<MockRunningTask> runningTasks) {
    this.restartAttemps = restartAttemps;
    this.startTime = startTime;
    this.allocatedEvalautors = allocatedEvalautors;
    this.activeContexts = activeContexts;
    this.runningTasks = runningTasks;
    this.failedEvaluators = new ArrayList<>();
  }

  /**
   * Generate a DriverRestarted event to be passed to the
   * {@link org.apache.reef.driver.parameters.DriverRestartHandler}.
   * @return DriverRestarted event based on the state at the time of driver failure
   */
  public DriverRestarted getDriverRestarted() {
    final Set<String> expectedEvaluatorIds = new HashSet<>();
    for (final MockAllocatedEvalautor allocatedEvalautor : this.allocatedEvalautors) {
      expectedEvaluatorIds.add(allocatedEvalautor.getId());
    }
    return new DriverRestarted() {
      @Override
      public int getResubmissionAttempts() {
        return restartAttemps;
      }

      @Override
      public StartTime getStartTime() {
        return startTime;
      }

      @Override
      public Set<String> getExpectedEvaluatorIds() {
        return expectedEvaluatorIds;
      }
    };
  }

  public DriverRestartCompleted getDriverRestartCompleted(final boolean isTimeout, final long restartDuration) {
    return new DriverRestartCompleted() {
      @Override
      public Time getCompletedTime() {
        return new StopTime(startTime.getTimestamp() + restartDuration);
      }

      @Override
      public boolean isTimedOut() {
        return isTimeout;
      }
    };
  }

  /**
   * Pass these tasks to the {@link org.apache.reef.driver.parameters.DriverRestartTaskRunningHandlers}.
   * @return MockRunningTasks at the time of driver failure
   */
  public List<MockRunningTask> getRunningTasks() {
    return this.runningTasks;
  }

  /**
   * Pass these active contexts to the {@link org.apache.reef.driver.parameters.DriverRestartContextActiveHandlers}.
   * These active contexts have no tasks running.
   * @return
   */
  public List<MockActiveContext> getIdleActiveContexts() {
    final List<MockActiveContext> idleActiveContexts = new ArrayList<>();
    final Set<String> activeContextsWithRunningTasks = new HashSet<>();
    for (final MockRunningTask task : this.runningTasks) {
      activeContextsWithRunningTasks.add(task.getActiveContext().getEvaluatorId());
    }
    for (final MockActiveContext context : this.activeContexts) {
      if (!activeContextsWithRunningTasks.contains(context.getEvaluatorId())) {
        idleActiveContexts.add(context);
      }
    }
    return idleActiveContexts;
  }

  public List<MockFailedEvaluator> getFailedEvaluators() {
    return this.failedEvaluators;
  }

  /**
   * Fail a task.
   * @param task to fail
   */
  public void failTask(final MockRunningTask task) {
    this.runningTasks.remove(task);
  }

  /**
   * Fail an evaluator; automatically cleans up state i.e., running tasks and contexts
   * pertaining to the evaluator, and adds the evaluator to {@link this#getFailedEvaluators()}, which
   * can be passed to the {@link org.apache.reef.driver.parameters.DriverRestartFailedEvaluatorHandlers}.
   * @param evalautor to fail
   */
  public void failEvaluator(final MockAllocatedEvalautor evalautor) {
    if (this.allocatedEvalautors.contains(evalautor)) {
      this.failedEvaluators.add(new MockFailedEvaluator(evalautor.getId()));
      // cleanup
      this.allocatedEvalautors.remove(evalautor);
      final Iterator<MockRunningTask> taskIter = this.runningTasks.iterator();
      while (taskIter.hasNext()) {
        final MockRunningTask task = taskIter.next();
        if (task.evaluatorID().equals(evalautor.getId())) {
          taskIter.remove();
        }
      }
      final Iterator<MockActiveContext> contextIter = this.activeContexts.iterator();
      while (contextIter.hasNext()) {
        final MockActiveContext context = contextIter.next();
        if (context.getEvaluatorId().equals(evalautor.getId())) {
          contextIter.remove();
        }
      }
    }
  }
}
