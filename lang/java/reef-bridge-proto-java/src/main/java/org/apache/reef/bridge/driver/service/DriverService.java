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
package org.apache.reef.bridge.driver.service;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

/**
 * Interface implemented by a Driver Service.
 */
@Private
public interface DriverService {

  /**
   * Driver restart handler.
   * @param restart event
   */
  void driverRestarted(DriverRestarted restart);

  /**
   * Restart running task.
   * @param task running
   */
  void restartRunningTask(RunningTask task);

  /**
   * Restart active context.
   * @param context restart
   */
  void restartActiveContext(ActiveContext context);

  /**
   * Driver restart completed.
   * @param restartCompleted event
   */
  void driverRestartCompleted(DriverRestartCompleted restartCompleted);

  /**
   * Failed to restart evaluator.
   * @param evaluator that failed.
   */
  void restartFailedEvalautor(FailedEvaluator evaluator);

  /**
   * Handle start time event.
   * @param startTime event
   */
  void startHandler(StartTime startTime);

  /**
   * Handle stop event.
   * @param stopTime event
   */
  void stopHandler(StopTime stopTime);

  /**
   * Handle allocated evaluator event.
   * @param eval allocated
   */
  void allocatedEvaluatorHandler(AllocatedEvaluator eval);

  /**
   * Handle completed evaluator event.
   * @param eval that completed
   */
  void completedEvaluatorHandler(CompletedEvaluator eval);

  /**
   * Handle failed evaluator event.
   * @param eval that failed
   */
  void failedEvaluatorHandler(FailedEvaluator eval);

  /**
   * Handle active context.
   * @param context activated
   */
  void activeContextHandler(ActiveContext context);

  /**
   * Handle closed context event.
   * @param context that closed
   */
  void closedContextHandler(ClosedContext context);

  /**
   * Handle context message event.
   * @param message sent by context
   */
  void contextMessageHandler(ContextMessage message);

  /**
   * Handled failed context event.
   * @param context that failed
   */
  void failedContextHandler(FailedContext context);

  /**
   * Handle running task event.
   * @param task that is now running
   */
  void runningTaskHandler(RunningTask task);

  /**
   * Handle failed task event.
   * @param task that failed
   */
  void failedTaskHandler(FailedTask task);

  /**
   * Handle completed task event.
   * @param task that completed
   */
  void completedTaskHandler(CompletedTask task);

  /**
   * Handle suspended task event.
   * @param task that is suspended
   */
  void suspendedTaskHandler(SuspendedTask task);

  /**
   * Handle task message event.
   * @param message sent by task
   */
  void taskMessageHandler(TaskMessage message);

  /**
   * Handle client message event.
   * @param message sent by client
   */
  void clientMessageHandler(byte[] message);

  /**
   * Handle client close event.
   */
  void clientCloseHandler();

  /**
   * Handle client close event with message.
   * @param message sent by client
   */
  void clientCloseWithMessageHandler(byte[] message);
}
