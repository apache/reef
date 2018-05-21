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

import org.apache.reef.bridge.driver.client.grpc.DriverServiceClient;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

import java.io.File;
import java.util.List;

/**
 * Forwards application requests to driver server.
 */
@DefaultImplementation(DriverServiceClient.class)
public interface IDriverServiceClient {

  void onInitializationException(final Throwable ex);

  /**
   * Initiate shutdown.
   */
  void onShutdown();

  /**
   * Initiate shutdown with error.
   * @param ex exception error
   */
  void onShutdown(final Throwable ex);

  /**
   * Set alarm.
   * @param alarmId alarm identifier
   * @param timeoutMS timeout in milliseconds
   */
  void onSetAlarm(final String alarmId, final int timeoutMS);

  /**
   * Request evaluators.
   * @param evaluatorRequest event
   */
  void onEvaluatorRequest(final EvaluatorRequest evaluatorRequest);

  /**
   * Close evaluator.
   * @param evalautorId to close
   */
  void onEvaluatorClose(final String evalautorId);

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
      final String evaluatorId,
      final Optional<Configuration> contextConfiguration,
      final Optional<Configuration> taskConfiguration,
      final Optional<JVMClientProcess> evaluatorProcess,
      final Optional<List<File>> addFileList,
      final Optional<List<File>> addLibraryList);

  // Context Operations

  /**
   * Close context.
   * @param contextId to close
   */
  void onContextClose(final String contextId);

  /**
   * Submit child context.
   * @param contextId to submit against
   * @param contextConfiguration for child context
   */
  void onContextSubmitContext(
      final String contextId,
      final Configuration contextConfiguration);

  /**
   * Submit task.
   * @param contextId to submit against
   * @param taskConfiguration for task
   */
  void onContextSubmitTask(
      final String contextId,
      final Configuration taskConfiguration);

  /**
   * Send message to context.
   * @param contextId to destination context
   * @param message to send
   */
  void onContextMessage(final String contextId, final byte[] message);

  // Task operations

  /**
   * Close the task.
   * @param taskId to close
   * @param message optional message to include
   */
  void onTaskClose(final String taskId, final Optional<byte[]> message);

  /**
   * Send task a message.
   * @param taskId of destination task
   * @param message to send
   */
  void onTaskMessage(final String taskId, final byte[] message);

  /**
   * Suspend a running task.
   * @param taskId task identifier
   * @param message optional message
   */
  void onSuspendTask(final String taskId, final Optional<byte[]> message);
}
