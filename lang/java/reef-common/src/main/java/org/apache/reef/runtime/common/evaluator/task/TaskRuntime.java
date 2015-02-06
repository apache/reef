/**
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
package org.apache.reef.runtime.common.evaluator.task;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.evaluator.HeartBeatManager;
import org.apache.reef.runtime.common.evaluator.task.exceptions.*;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.task.events.SuspendEvent;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The execution environment for a Task.
 */
@Private
@EvaluatorSide
public final class TaskRuntime implements Runnable {

  private final static Logger LOG = Logger.getLogger(TaskRuntime.class.getName());

  /**
   * User supplied Task code.
   */
  private final Task task;

  private final InjectionFuture<EventHandler<CloseEvent>> f_closeHandler;
  private final InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler;
  private final InjectionFuture<EventHandler<DriverMessage>> f_messageHandler;
  private final TaskLifeCycleHandlers taskLifeCycleHandlers;

  /**
   * The memento given by the task configuration.
   */
  private final Optional<byte[]> memento;

  /**
   * Heart beat manager to trigger on heartbeats.
   */
  private final HeartBeatManager heartBeatManager;

  private final TaskStatus currentStatus;

  // TODO: Document
  @Inject
  private TaskRuntime(
      final HeartBeatManager heartBeatManager,
      final Task task,
      final TaskStatus currentStatus,
      final @Parameter(TaskConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
      final @Parameter(TaskConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
      final @Parameter(TaskConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler,
      final TaskLifeCycleHandlers taskLifeCycleHandlers) {
    this(heartBeatManager, task, currentStatus, f_closeHandler, f_suspendHandler, f_messageHandler, null, taskLifeCycleHandlers);
  }

  // TODO: Document
  @Inject
  private TaskRuntime(
      final HeartBeatManager heartBeatManager,
      final Task task,
      final TaskStatus currentStatus,
      final @Parameter(TaskConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
      final @Parameter(TaskConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
      final @Parameter(TaskConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler,
      final @Parameter(TaskConfigurationOptions.Memento.class) String memento,
      final TaskLifeCycleHandlers taskLifeCycleHandlers) {

    this.heartBeatManager = heartBeatManager;
    this.task = task;
    this.taskLifeCycleHandlers = taskLifeCycleHandlers;

    this.memento = null == memento ? Optional.<byte[]>empty() :
        Optional.of(DatatypeConverter.parseBase64Binary(memento));

    this.f_closeHandler = f_closeHandler;
    this.f_suspendHandler = f_suspendHandler;
    this.f_messageHandler = f_messageHandler;

    this.currentStatus = currentStatus;
  }

  /**
   * This method needs to be called before a Task can be run().
   * It informs the Driver that the Task is initializing.
   */
  public void initialize() {
    this.currentStatus.setInit();
  }

  /**
   * Run the task: Fire TaskStart, call Task.call(), fire TaskStop.
   */
  @Override
  public void run() {
    try {
      // Change state and inform the Driver
      this.taskLifeCycleHandlers.beforeTaskStart();

      LOG.log(Level.FINEST, "Informing registered EventHandler<TaskStart>.");
      this.currentStatus.setRunning();

      // Call Task.call()
      final byte[] result = this.runTask();

      // Inform the Driver about it
      this.currentStatus.setResult(result);

      LOG.log(Level.FINEST, "Informing registered EventHandler<TaskStop>.");
      this.taskLifeCycleHandlers.afterTaskExit();

    } catch (final TaskStartHandlerFailure taskStartHandlerFailure) {
      LOG.log(Level.WARNING, "Caught an exception during TaskStart handler execution.", taskStartHandlerFailure);
      this.currentStatus.setException(taskStartHandlerFailure.getCause());
    } catch (final TaskStopHandlerFailure taskStopHandlerFailure) {
      LOG.log(Level.WARNING, "Caught an exception during TaskStop handler execution.", taskStopHandlerFailure);
      this.currentStatus.setException(taskStopHandlerFailure.getCause());
    } catch (final TaskCallFailure e) {
      LOG.log(Level.WARNING, "Caught an exception during Task.call().", e.getCause());
      this.currentStatus.setException(e);
    }
  }

  /**
   * Called by heartbeat manager
   *
   * @return current TaskStatusProto
   */
  public ReefServiceProtos.TaskStatusProto getStatusProto() {
    return this.currentStatus.toProto();
  }

  /**
   * @return true, if the Task is no longer running, either because it is crashed or exited cleanly
   */
  public boolean hasEnded() {
    return this.currentStatus.hasEnded();
  }

  /**
   * @return the ID of the task.
   */
  public String getTaskId() {
    return this.currentStatus.getTaskId();
  }

  public String getId() {
    return "TASK:" + this.task.getClass().getSimpleName() + ':' + this.currentStatus.getTaskId();
  }

  /**
   * Close the Task. This calls the configured close handler.
   *
   * @param message the optional message for the close handler or null if there none.
   */
  public final void close(final byte[] message) {
    LOG.log(Level.FINEST, "Triggering Task close.");
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to close a task that is in state: {0}. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.closeTask(message);
          this.currentStatus.setCloseRequested();
        } catch (final TaskCloseHandlerFailure taskCloseHandlerFailure) {
          LOG.log(Level.WARNING, "Exception while executing task close handler.",
              taskCloseHandlerFailure.getCause());
          this.currentStatus.setException(taskCloseHandlerFailure.getCause());
        }
      }
    }
  }

  /**
   * Suspend the Task.  This calls the configured suspend handler.
   *
   * @param message the optional message for the suspend handler or null if there none.
   */
  public void suspend(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to suspend a task that is in state: {0}. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.suspendTask(message);
          this.currentStatus.setSuspendRequested();
        } catch (final TaskSuspendHandlerFailure taskSuspendHandlerFailure) {
          LOG.log(Level.WARNING, "Exception while executing task suspend handler.",
              taskSuspendHandlerFailure.getCause());
          this.currentStatus.setException(taskSuspendHandlerFailure.getCause());
        }
      }
    }
  }

  /**
   * Deliver a message to the Task. This calls into the user supplied message handler.
   *
   * @param message the message to be delivered.
   */
  public void deliver(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING,
            "Trying to send a message to a task that is in state: {0}. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.deliverMessageToTask(message);
        } catch (final TaskMessageHandlerFailure taskMessageHandlerFailure) {
          LOG.log(Level.WARNING, "Exception while executing task close handler.",
              taskMessageHandlerFailure.getCause());
          this.currentStatus.setException(taskMessageHandlerFailure.getCause());
        }
      }
    }
  }

  /**
   * @return the ID of the Context this task is executing in.
   */
  private String getContextID() {
    return this.currentStatus.getContextId();
  }

  /**
   * Calls the Task.call() method and catches exceptions it may throw.
   *
   * @return the return value of Task.call()
   * @throws TaskCallFailure if any Throwable was caught from the Task.call() method.
   *                         That throwable would be the cause of the TaskCallFailure.
   */
  private byte[] runTask() throws TaskCallFailure {
    try {
      final byte[] result;
      if (this.memento.isPresent()) {
        LOG.log(Level.FINEST, "Calling Task.call() with a memento");
        result = this.task.call(this.memento.get());
      } else {
        LOG.log(Level.FINEST, "Calling Task.call() without a memento");
        result = this.task.call(null);
      }
      LOG.log(Level.FINEST, "Task.call() exited cleanly.");
      return result;
    } catch (final Throwable throwable) {
      throw new TaskCallFailure(throwable);
    }
  }

  /**
   * Calls the configured Task close handler and catches exceptions it may throw.
   */
  private void closeTask(final byte[] message) throws TaskCloseHandlerFailure {
    LOG.log(Level.FINEST, "Invoking close handler.");
    try {
      this.f_closeHandler.get().onNext(new CloseEventImpl(message));
    } catch (final Throwable throwable) {
      throw new TaskCloseHandlerFailure(throwable);
    }
  }

  /**
   * Calls the configured Task message handler and catches exceptions it may throw.
   */
  private void deliverMessageToTask(final byte[] message) throws TaskMessageHandlerFailure {
    try {
      this.f_messageHandler.get().onNext(new DriverMessageImpl(message));
    } catch (final Throwable throwable) {
      throw new TaskMessageHandlerFailure(throwable);
    }
  }

  /**
   * Calls the configured Task suspend handler and catches exceptions it may throw.
   */
  private void suspendTask(final byte[] message) throws TaskSuspendHandlerFailure {
    try {
      this.f_suspendHandler.get().onNext(new SuspendEventImpl(message));
    } catch (final Throwable throwable) {
      throw new TaskSuspendHandlerFailure(throwable);
    }
  }
}
