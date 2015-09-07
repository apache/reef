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
package org.apache.reef.io.watcher;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.*;
import org.apache.reef.io.watcher.param.EventStreams;
import org.apache.reef.io.watcher.util.WatcherAvroUtil;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.Set;

/**
 * Subscribe events and transfer them as wrapping with corresponding avro classes.
 */
@Unstable
@Unit
public final class Watcher {

  private final Set<EventStream> eventStreamSet;

  @Inject
  private Watcher(@Parameter(EventStreams.class) final Set<EventStream> eventStreamSet) {
    this.eventStreamSet = eventStreamSet;
  }

  private void onEvent(final EventType eventType, final String jsonEncodedEvent) {
    for (final EventStream eventStream : eventStreamSet) {
      eventStream.onEvent(eventType, jsonEncodedEvent);
    }
  }

  public final class DriverRuntimeStartHandler implements EventHandler<RuntimeStart> {

    @Override
    public void onNext(final RuntimeStart runtimeStart) {
      onEvent(EventType.RuntimeStart, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroRuntimeStart(runtimeStart)));
    }
  }

  public final class DriverStartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      onEvent(EventType.StartTime, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroStartTime(startTime)));
    }
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      onEvent(EventType.StopTime, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroStopTime(stopTime)));
    }
  }

  public final class DriverRuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public void onNext(final RuntimeStop runtimeStop) {
      onEvent(EventType.RuntimeStop, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroRuntimeStop(runtimeStop)));
    }
  }

  public final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      onEvent(EventType.ActiveContext, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroActiveContext(activeContext)));
    }
  }

  public final class ContextClosedHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      onEvent(EventType.ClosedContext, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroClosedContext(closedContext)));
    }
  }

  public final class ContextFailedHandler implements EventHandler<FailedContext> {

    @Override
    public void onNext(final FailedContext failedContext) {
      onEvent(EventType.FailedContext, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroFailedContext(failedContext)));
    }
  }

  public final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      onEvent(EventType.CompletedTask, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroCompletedTask(completedTask)));
    }
  }

  public final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      onEvent(EventType.FailedTask, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroFailedTask(failedTask)));
    }
  }

  public final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      onEvent(EventType.RunningTask, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroRunningTask(runningTask)));
    }
  }

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {

    @Override
    public void onNext(final TaskMessage taskMessage) {
      onEvent(EventType.TaskMessage, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroTaskMessage(taskMessage)));
    }
  }

  public final class TaskSuspendedHandler implements EventHandler<SuspendedTask> {

    @Override
    public void onNext(final SuspendedTask suspendedTask) {
      onEvent(EventType.SuspendedTask, WatcherAvroUtil.toString(WatcherAvroUtil.toAvroSuspendedTask(suspendedTask)));
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      onEvent(EventType.AllocatedEvaluator,
          WatcherAvroUtil.toString(WatcherAvroUtil.toAvroAllocatedEvaluator(allocatedEvaluator)));
    }
  }

  public final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      onEvent(EventType.FailedEvaluator,
          WatcherAvroUtil.toString(WatcherAvroUtil.toAvroFailedEvaluator(failedEvaluator)));
    }
  }

  public final class EvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {

    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      onEvent(EventType.CompletedEvaluator,
          WatcherAvroUtil.toString(WatcherAvroUtil.toAvroCompletedEvaluator(completedEvaluator)));
    }
  }
}
