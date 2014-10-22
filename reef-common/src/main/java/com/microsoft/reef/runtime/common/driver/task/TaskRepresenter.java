/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.task;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.context.EvaluatorContext;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.util.Optional;

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

  // Mutable state
  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;

  public TaskRepresenter(final String taskId,
                         final EvaluatorContext context,
                         final EvaluatorMessageDispatcher messageDispatcher,
                         final EvaluatorManager evaluatorManager,
                         final ExceptionCodec exceptionCodec) {
    this.taskId = taskId;
    this.context = context;
    this.messageDispatcher = messageDispatcher;
    this.evaluatorManager = evaluatorManager;
    this.exceptionCodec = exceptionCodec;
  }

  public void onTaskStatusMessage(final ReefServiceProtos.TaskStatusProto taskStatusProto) {

    LOG.log(Level.FINE, "Received task {0} status {1}",
        new Object[]{taskStatusProto.getTaskId(), taskStatusProto.getState()});

    // Make sure that the message is indeed for us.
    if (!taskStatusProto.getContextId().equals(this.context.getId())) {
      throw new RuntimeException(
          "Received a message for a task running on Context " + taskStatusProto.getContextId() +
              " while the Driver believes this Task to be run on Context " + this.context.getId());
    }

    if (!taskStatusProto.getTaskId().equals(this.taskId)) {
      throw new RuntimeException("Received a message for task " + taskStatusProto.getTaskId() +
          " in the TaskRepresenter for Task " + this.taskId);
    }
    if(taskStatusProto.getRecovery())
    {
      // when a recovered heartbeat is received, we will take its word for it
      LOG.log(Level.INFO, "Received task status {0} for RECOVERED task {1}.",
          new Object[]{ taskStatusProto.getState(), this.taskId });
      this.setState(taskStatusProto.getState());
    }
    // Dispatch the message to the right method.
    switch (taskStatusProto.getState()) {
      case INIT:
        this.onTaskInit(taskStatusProto);
        break;
      case RUNNING:
        this.onTaskRunning(taskStatusProto);
        break;
      case SUSPEND:
        this.onTaskSuspend(taskStatusProto);
        break;
      case DONE:
        this.onTaskDone(taskStatusProto);
        break;
      case FAILED:
        this.onTaskFailed(taskStatusProto);
        break;
      default:
        throw new IllegalStateException("Unknown task state: " + taskStatusProto.getState());
    }
  }

  private void onTaskInit(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    assert ((ReefServiceProtos.State.INIT == taskStatusProto.getState()));
    if (this.isKnown()) {
      LOG.log(Level.WARNING, "Received a INIT message for task with id {0}" +
          " which we have seen before. Ignoring the second message", this.taskId);
    } else {
      final RunningTask runningTask = new RunningTaskImpl(
          this.evaluatorManager, this.taskId, this.context, this);
      this.messageDispatcher.onTaskRunning(runningTask);
      this.setState(ReefServiceProtos.State.RUNNING);
    }
  }

  private void onTaskRunning(final ReefServiceProtos.TaskStatusProto taskStatusProto) {

    assert (taskStatusProto.getState() == ReefServiceProtos.State.RUNNING);

    if (this.isNotRunning()) {
      throw new IllegalStateException("Received a task status message from task " + this.taskId +
          " that is believed to be RUNNING on the Evaluator, but the Driver thinks it is in state " + this.state);
    }

    // fire driver restart task running handler if this is a recovery heartbeat
    if(taskStatusProto.getRecovery())
    {
      final RunningTask runningTask = new RunningTaskImpl(
          this.evaluatorManager, this.taskId, this.context, this);
      this.messageDispatcher.onDriverRestartTaskRunning(runningTask);
    }

    for (final ReefServiceProtos.TaskStatusProto.TaskMessageProto taskMessageProto : taskStatusProto.getTaskMessageList()) {
      this.messageDispatcher.onTaskMessage(
          new TaskMessageImpl(taskMessageProto.getMessage().toByteArray(),
              this.taskId, this.context.getId(), taskMessageProto.getSourceId()));
    }
  }

  private void onTaskSuspend(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    assert (ReefServiceProtos.State.SUSPEND == taskStatusProto.getState());
    assert (this.isKnown());
    this.messageDispatcher.onTaskSuspended(
        new SuspendedTaskImpl(this.context, getResult(taskStatusProto), this.taskId));
    this.setState(ReefServiceProtos.State.SUSPEND);
  }

  private void onTaskDone(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    assert (ReefServiceProtos.State.DONE == taskStatusProto.getState());
    assert (this.isKnown());
    this.messageDispatcher.onTaskCompleted(
        new CompletedTaskImpl(this.context, getResult(taskStatusProto), this.taskId));
    this.setState(ReefServiceProtos.State.DONE);
  }

  private void onTaskFailed(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    assert (ReefServiceProtos.State.FAILED == taskStatusProto.getState());
    final Optional<ActiveContext> evaluatorContext = Optional.<ActiveContext>of(this.context);
    final Optional<byte[]> bytes = Optional.ofNullable(getResult(taskStatusProto));
    final Optional<Throwable> exception = this.exceptionCodec.fromBytes(bytes);
    final String message = exception.isPresent() ? exception.get().getMessage() : "No message given";
    final Optional<String> description = Optional.empty();
    final FailedTask failedTask = new FailedTask(
        this.taskId, message, description, exception, bytes, evaluatorContext);
    this.messageDispatcher.onTaskFailed(failedTask);
    this.setState(ReefServiceProtos.State.FAILED);
  }

  private static byte[] getResult(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    return taskStatusProto.hasResult() ? taskStatusProto.getResult().toByteArray() : null;
  }

  public String getId() {
    return this.taskId;
  }

  /**
   * @return true, if we had at least one message from the task.
   */
  private boolean isKnown() {
    return this.state != ReefServiceProtos.State.INIT;
  }

  /**
   * @return true, if this task is in any other state but RUNNING.
   */
  public boolean isNotRunning() {
    return this.state != ReefServiceProtos.State.RUNNING;
  }

  private void setState(final ReefServiceProtos.State newState) {
    LOG.log(Level.FINE, "Task [{0}] state transition from [{1}] to [{2}]",
        new Object[]{this.taskId, this.state, newState});
    this.state = newState;
  }
}
