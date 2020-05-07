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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

import java.io.File;
import java.util.List;

/**
 * Forwards application requests to driver server.
 */
@Private
public interface DriverServiceClient {

  void onInitializationException(Throwable ex);

  /**
   * Initiate shutdown.
   */
  void onShutdown();

  /**
   * Initiate shutdown with error.
   * @param ex exception error
   */
  void onShutdown(Throwable ex);

  /**
   * Set alarm.
   * @param alarmId alarm identifier
   * @param timeoutMS timeout in milliseconds
   */
  void onSetAlarm(String alarmId, int timeoutMS);

  /**
   * Request evaluators.
   * @param evaluatorRequest event
   */
  void onEvaluatorRequest(EvaluatorRequest evaluatorRequest);

  /**
   * Close evaluator.
   * @param evalautorId to close
   */
  void onEvaluatorClose(String evalautorId);

  /**
   * Submit context and/or task.
   * @param evaluatorId to submit against
   * @param contextConfiguration context configuration
   * @param taskConfiguration task configuration
   * @param evaluatorProcess evaluator process
   * @param addFileList to include
   * @param addLibraryList to include
   */
  void onEvaluatorSubmit(
      String evaluatorId,
      Optional<Configuration> contextConfiguration,
      Optional<Configuration> taskConfiguration,
      Optional<JVMClientProcess> evaluatorProcess,
      List<File> addFileList,
      List<File> addLibraryList);

  // Context Operations

  /**
   * Close context.
   * @param contextId to close
   */
  void onContextClose(String contextId);

  /**
   * Submit child context.
   * @param contextId to submit against
   * @param contextConfiguration for child context
   */
  void onContextSubmitContext(
      String contextId,
      Configuration contextConfiguration);

  /**
   * Submit task.
   * @param contextId to submit against
   * @param taskConfiguration for task
   */
  void onContextSubmitTask(
      String contextId,
      Configuration taskConfiguration);

  /**
   * Send message to context.
   * @param contextId to destination context
   * @param message to send
   */
  void onContextMessage(String contextId, byte[] message);

  // Task operations

  /**
   * Close the task.
   * @param taskId to close
   * @param message optional message to include
   */
  void onTaskClose(String taskId, Optional<byte[]> message);

  /**
   * Send task a message.
   * @param taskId of destination task
   * @param message to send
   */
  void onTaskMessage(String taskId, byte[] message);

  /**
   * Suspend a running task.
   * @param taskId task identifier
   * @param message optional message
   */
  void onSuspendTask(String taskId, Optional<byte[]> message);
}
