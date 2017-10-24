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

package org.apache.reef.mock;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * mock application.
 */
@Unit
final class MockApplication {

  private static final Logger LOG = Logger.getLogger(MockApplication.class.getName());

  private final Clock clock;

  private final EvaluatorRequestor evaluatorRequestor;

  private final Map<String, Map<String, ActiveContext>> evaluatorId2ContextId2ContextMap = new HashMap<>();

  private final Map<String, AllocatedEvaluator> evaluatorMap = new HashMap<>();

  private final Map<String, FailedEvaluator> failedEvaluatorMap = new HashMap<>();

  private final Map<String, RunningTask> evaluatorIdRunningTaskMap = new HashMap<>();

  private final Set<FailedContext> failedContextSet = new HashSet<>();

  private final Set<FailedTask> failedTaskSet = new HashSet<>();

  private final Set<SuspendedTask> suspendedTaskSet = new HashSet<>();

  private boolean running = false;

  @Inject
  MockApplication(final Clock clock, final EvaluatorRequestor evaluatorRequestor) {
    this.clock = clock;
    this.evaluatorRequestor = evaluatorRequestor;
  }

  ActiveContext getContext(final AllocatedEvaluator evaluator, final String identifier) {
    return this.evaluatorId2ContextId2ContextMap.get(evaluator.getId()).get(identifier);
  }

  Collection<RunningTask> getRunningTasks() {
    return Collections.unmodifiableCollection(this.evaluatorIdRunningTaskMap.values());
  }

  Collection<AllocatedEvaluator> getAllocatedEvaluators() {
    return Collections.unmodifiableCollection(this.evaluatorMap.values());
  }

  Collection<FailedEvaluator> getFailedEvaluators() {
    return Collections.unmodifiableCollection(this.failedEvaluatorMap.values());
  }

  Collection<FailedTask> getFailedTasks() {
    return Collections.unmodifiableCollection(this.failedTaskSet);
  }

  Collection<FailedContext> getFailedContext() {
    return Collections.unmodifiableCollection(this.failedContextSet);
  }

  void requestEvaluators(final int numEvaluators) {
    LOG.log(Level.INFO, "request {0} Evaluators", numEvaluators);
    evaluatorRequestor.newRequest()
        .setMemory(128)
        .setNumberOfCores(1)
        .setNumber(numEvaluators)
        .submit();
  }

  void submitTask(final ActiveContext context, final String identifier) {
    context.submitTask(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, identifier)
        .set(TaskConfiguration.TASK, DummyTestTask.class)
        .build());
  }

  void submitContext(final ActiveContext context, final String identifier) {
    context.submitContext(ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, identifier)
        .build());
  }

  boolean isRunning() {
    return this.running;
  }

  boolean exists(final AllocatedEvaluator evaluator) {
    return this.evaluatorMap.containsKey(evaluator.getId());
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluatorMap.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      clock.scheduleAlarm(Integer.MAX_VALUE, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm value) {
          throw new RuntimeException("should not happen");
        }
      });
      running = true;
    }
  }

  /**
   * Job Driver is is shutting down: write to the log.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      running = false;
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      evaluatorMap.put(eval.getId(), eval);
    }
  }

  /**
   * Receive notification that the Evaluator has been shut down.
   */
  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      evaluatorMap.remove(eval.getId());
      evaluatorId2ContextId2ContextMap.remove(eval.getId());
      evaluatorIdRunningTaskMap.remove(eval.getId());
    }
  }

  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator eval) {
      evaluatorMap.remove(eval.getId());
      evaluatorId2ContextId2ContextMap.remove(eval.getId());
      evaluatorIdRunningTaskMap.remove(eval.getId());
      failedEvaluatorMap.put(eval.getId(), eval);
      failedContextSet.addAll(eval.getFailedContextList());
    }
  }

  /**
   * Receive notification that the Context is active.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      if (!evaluatorId2ContextId2ContextMap.containsKey(context.getEvaluatorId())) {
        evaluatorId2ContextId2ContextMap.put(context.getEvaluatorId(), new HashMap<String, ActiveContext>());
      }
      if (evaluatorId2ContextId2ContextMap.get(context.getEvaluatorId()).containsKey(context.getId())) {
        throw new IllegalStateException(
                String.format("Context %s on evaluator %s already exists on evaluator with " +
                        "same identifier", context.getId(), context.getEvaluatorId()));
      }
      evaluatorId2ContextId2ContextMap.get(context.getEvaluatorId()).put(context.getId(), context);
    }
  }

  final class ContextClosedHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext value) {
      assert evaluatorId2ContextId2ContextMap.containsKey(value.getEvaluatorId());
      assert evaluatorId2ContextId2ContextMap.get(value.getEvaluatorId()).containsKey(value.getId());
      evaluatorId2ContextId2ContextMap.get(value.getEvaluatorId()).remove(value.getId());
    }
  }

  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext value) {
      if (evaluatorId2ContextId2ContextMap.containsKey(value.getEvaluatorId()) &&
          evaluatorId2ContextId2ContextMap.get(value.getEvaluatorId()).containsKey(value.getId())) {
        evaluatorId2ContextId2ContextMap.get(value.getEvaluatorId()).remove(value.getEvaluatorId());
      } else {
        // must have failed before it succeeded
      }
      failedContextSet.add(value);
    }
  }

  /**
   * Receive notification that the Task is running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      evaluatorIdRunningTaskMap.put(task.getActiveContext().getEvaluatorId(), task);
    }
  }

  /**
   * Receive notification that the Task has completed successfully.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      evaluatorIdRunningTaskMap.remove(task.getActiveContext().getEvaluatorId());
    }
  }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask value) {
      evaluatorIdRunningTaskMap.remove(value.getActiveContext().get().getEvaluatorId());
      failedTaskSet.add(value);
    }
  }

  final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    public void onNext(final SuspendedTask value) {
      evaluatorIdRunningTaskMap.remove(value.getActiveContext().getEvaluatorId());
      suspendedTaskSet.add(value);
    }
  }

  private static final class DummyTestTask implements Task {
    @Override
    public byte[] call(final byte[] memento) throws Exception {
      return new byte[0];
    }
  }
}
