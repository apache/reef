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
package org.apache.reef.bridge.service;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains Java side event handlers that perform
 * hand-off with the driver client side.
 */
@Unit
public final class DriverServiceHandlers {

  private static final Logger LOG = Logger.getLogger(DriverServiceHandlers.class.getName());

  private final IDriverService driverBridgeService;

  @Inject
  private DriverServiceHandlers(
      final IDriverService driverBridgeService) {
    this.driverBridgeService = driverBridgeService;
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "JavaBridge: Start Driver");
      DriverServiceHandlers.this.driverBridgeService.startHandler(startTime);
    }
  }

  /**
   * Job Driver is is shutting down: write to the log.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOG.log(Level.INFO, "JavaBridge: Stop Driver");
      DriverServiceHandlers.this.driverBridgeService.stopHandler(stopTime);
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.INFO, "JavaBridge: Allocated Evaluator {0}", eval.getId());
      DriverServiceHandlers.this.driverBridgeService.allocatedEvaluatorHandler(eval);
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      LOG.log(Level.INFO, "JavaBridge: Completed Evaluator {0}", eval.getId());
      DriverServiceHandlers.this.driverBridgeService.completedEvaluatorHandler(eval);
    }
  }

  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator eval) {
      LOG.log(Level.INFO, "JavaBridge: Failed Evaluator {0}", eval.getId());
      DriverServiceHandlers.this.driverBridgeService.failedEvaluatorHandler(eval);
    }
  }

  /**
   * Receive notification that the Context is active.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "JavaBridge: Active Context {0}", context.getId());
      DriverServiceHandlers.this.driverBridgeService.activeContextHandler(context);
    }
  }

  /**
   * Received notification that the Context is closed.
   */
  final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext context) {
      LOG.log(Level.INFO, "JavaBridge: Closed Context {0}", context.getId());
      DriverServiceHandlers.this.driverBridgeService.closedContextHandler(context);
    }
  }

  /**
   * Received a message from the context.
   */
  final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage message) {
      LOG.log(Level.INFO, "JavaBridge: Context Message id {0}", message.getId());
      DriverServiceHandlers.this.driverBridgeService.contextMessageHandler(message);
    }
  }

  /**
   * Received notification that the Context failed.
   */
  final class ContextFailedHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext context) {
      LOG.log(Level.INFO, "JavaBridge: Context Failed {0}", context.getId());
      DriverServiceHandlers.this.driverBridgeService.failedContextHandler(context);
    }
  }

  /**
   * Receive notification that the Task is running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOG.log(Level.INFO, "JavaBridge: Running Task {0}", task.getId());
      DriverServiceHandlers.this.driverBridgeService.runningTaskHandler(task);
    }
  }

  /**
   * Received notification that the Task failed.
   */
  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) {
      LOG.log(Level.INFO, "JavaBridge: Failed Task {0}", task.getId());
      DriverServiceHandlers.this.driverBridgeService.failedTaskHandler(task);
    }
  }

  /**
   * Receive notification that the Task has completed successfully.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      LOG.log(Level.INFO, "JavaBridge: Completed Task {0}", task.getId());
      DriverServiceHandlers.this.driverBridgeService.completedTaskHandler(task);
    }
  }

  /**
   * Received notification that the Task was suspended.
   */
  final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    public void onNext(final SuspendedTask task) {
      LOG.log(Level.INFO, "JavaBridge: Suspended Task {0}", task.getId());
      DriverServiceHandlers.this.driverBridgeService.suspendedTaskHandler(task);
    }
  }

  /**
   * Received a message from the task.
   */
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage message) {
      LOG.log(Level.INFO, "JavaBridge: Message from Task {0}", message.getId());
      DriverServiceHandlers.this.driverBridgeService.taskMessageHandler(message);
    }
  }

  /**
   * Received a message from the client.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      LOG.log(Level.INFO, "JavaBridge: Message from Client");
      DriverServiceHandlers.this.driverBridgeService.clientMessageHandler(message);
    }
  }

  /**
   * Received a close event from the client.
   */
  final class ClientCloseHandler implements EventHandler<Void> {
    @Override
    public void onNext(final Void value) {
      LOG.log(Level.INFO, "JavaBridge: Close event from Client");
      DriverServiceHandlers.this.driverBridgeService.clientCloseHandler();
    }
  }

  /**
   * Received a close event with message.
   */
  final class ClientCloseWithMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      LOG.log(Level.INFO, "JavaBridge: Close event with messages from Client");
      DriverServiceHandlers.this.driverBridgeService.clientCloseWithMessageHandler(message);
    }
  }
}
