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
package org.apache.reef.runtime.common.driver.task;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.driver.restart.EvaluatorRestartState;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.evaluator.pojos.TaskMessagePOJO;
import org.apache.reef.runtime.common.driver.evaluator.pojos.TaskStatusPOJO;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.util.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a Task on the Driver.
 */
@DriverSide
@Private
public final class TaskRepresenter {

  private static final Logger LOG = Logger.getLogger(TaskRepresenter.class.getName());

  private final EvaluatorContext context;
  private final EvaluatorMessageDispatcher messageDispatcher;
  private final EvaluatorManager evaluatorManager;
  private final ExceptionCodec exceptionCodec;
  private final String taskId;
  private final DriverRestartManager driverRestartManager;

  // Mutable state
  private State state = State.INIT;

  public TaskRepresenter(final String taskId,
                         final EvaluatorContext context,
                         final EvaluatorMessageDispatcher messageDispatcher,
                         final EvaluatorManager evaluatorManager,
                         final ExceptionCodec exceptionCodec,
                         final DriverRestartManager driverRestartManager) {
    this.taskId = taskId;
    this.context = context;
    this.messageDispatcher = messageDispatcher;
    this.evaluatorManager = evaluatorManager;
    this.exceptionCodec = exceptionCodec;
    this.driverRestartManager = driverRestartManager;
  }

  private static byte[] getResult(final TaskStatusPOJO taskStatus) {
    return taskStatus.hasResult() ? taskStatus.getResult() : null;
  }

  public void onTaskStatusMessage(final TaskStatusPOJO taskStatus) {

    LOG.log(Level.FINE, "Received task {0} status {1}",
            new Object[]{taskStatus.getTaskId(), taskStatus.getState()});

    // Make sure that the message is indeed for us.
    if (!taskStatus.getContextId().equals(this.context.getId())) {
      throw new RuntimeException(
          "Received a message for a task running on Context " + taskStatus.getContextId() +
              " while the Driver believes this Task to be run on Context " + this.context.getId());
    }

    if (!taskStatus.getTaskId().equals(this.taskId)) {
      throw new RuntimeException("Received a message for task " + taskStatus.getTaskId() +
          " in the TaskRepresenter for Task " + this.taskId);
    }

    if (driverRestartManager.getEvaluatorRestartState(evaluatorManager.getId()) == EvaluatorRestartState.REREGISTERED) {
      // when a recovered heartbeat is received, we will take its word for it
      LOG.log(Level.INFO, "Received task status {0} for RECOVERED task {1}.",
          new Object[]{taskStatus.getState(), this.taskId});
      this.setState(taskStatus.getState());
    }
    // Dispatch the message to the right method.
    switch (taskStatus.getState()) {
    case INIT:
      this.onTaskInit(taskStatus);
      break;
    case RUNNING:
      this.onTaskRunning(taskStatus);
      break;
    case SUSPEND:
      this.onTaskSuspend(taskStatus);
      break;
    case DONE:
      this.onTaskDone(taskStatus);
      break;
    case FAILED:
      this.onTaskFailed(taskStatus);
      break;
    default:
      throw new IllegalStateException("Unknown task state: " + taskStatus.getState());
    }
  }

  private void onTaskInit(final TaskStatusPOJO taskStatusPOJO) {
    assert State.INIT == taskStatusPOJO.getState();
    if (this.isKnown()) {
      LOG.log(Level.WARNING, "Received a INIT message for task with id {0}" +
          " which we have seen before. Ignoring the second message", this.taskId);
    } else {
      final RunningTask runningTask = new RunningTaskImpl(
          this.evaluatorManager, this.taskId, this.context, this);
      this.messageDispatcher.onTaskRunning(runningTask);
      this.setState(State.RUNNING);
    }
  }

  private void onTaskRunning(final TaskStatusPOJO taskStatus) {
    assert taskStatus.getState() == State.RUNNING;

    if (this.isNotRunning()) {
      throw new IllegalStateException("Received a task status message from task " + this.taskId +
          " that is believed to be RUNNING on the Evaluator, but the Driver thinks it is in state " + this.state);
    }

    // fire driver restart task running handler if this is a recovery heartbeat
    if (driverRestartManager.getEvaluatorRestartState(evaluatorManager.getId()) == EvaluatorRestartState.REREGISTERED) {
      final RunningTask runningTask = new RunningTaskImpl(
          this.evaluatorManager, this.taskId, this.context, this);
      this.driverRestartManager.setEvaluatorProcessed(evaluatorManager.getId());
      this.messageDispatcher.onDriverRestartTaskRunning(runningTask);
    }

    for (final TaskMessagePOJO
             taskMessagePOJO : taskStatus.getTaskMessageList()) {
      this.messageDispatcher.onTaskMessage(
          new TaskMessageImpl(taskMessagePOJO.getMessage(),
              this.taskId, this.context.getId(), taskMessagePOJO.getSourceId(), taskMessagePOJO.getSequenceNumber()));
    }
  }

  private void onTaskSuspend(final TaskStatusPOJO taskStatus) {
    assert State.SUSPEND == taskStatus.getState();
    assert this.isKnown();
    this.messageDispatcher.onTaskSuspended(
        new SuspendedTaskImpl(this.context, getResult(taskStatus), this.taskId));
    this.setState(State.SUSPEND);
  }

  private void onTaskDone(final TaskStatusPOJO taskStatus) {
    assert State.DONE == taskStatus.getState();
    assert this.isKnown();
    this.messageDispatcher.onTaskCompleted(
        new CompletedTaskImpl(this.context, getResult(taskStatus), this.taskId));
    this.setState(State.DONE);
  }

  private void onTaskFailed(final TaskStatusPOJO taskStatus) {
    assert State.FAILED == taskStatus.getState();
    final Optional<ActiveContext> evaluatorContext = Optional.<ActiveContext>of(this.context);
    final Optional<byte[]> bytes = Optional.ofNullable(getResult(taskStatus));
    final Optional<Throwable> exception = this.exceptionCodec.fromBytes(bytes);
    final String message = exception.isPresent() ? exception.get().getMessage() : "No message given";
    final Optional<String> description = Optional.empty();
    final FailedTask failedTask = new FailedTask(
        this.taskId, message, description, exception, bytes, evaluatorContext);
    this.messageDispatcher.onTaskFailed(failedTask);
    this.setState(State.FAILED);
  }

  public String getId() {
    return this.taskId;
  }

  /**
   * @return true, if we had at least one message from the task.
   */
  private boolean isKnown() {
    return this.state != State.INIT;
  }

  /**
   * @return true, if this task is in any other state but RUNNING.
   */
  public boolean isNotRunning() {
    return this.state != State.RUNNING;
  }

  /**
   * @return true, if this task is in INIT or RUNNING status.
   */
  public boolean isClosable() {
    return this.state == State.INIT || this.state == State.RUNNING;
  }

  private void setState(final State newState) {
    LOG.log(Level.FINE, "Task [{0}] state transition from [{1}] to [{2}]",
        new Object[]{this.taskId, this.state, newState});
    this.state = newState;
  }
}
