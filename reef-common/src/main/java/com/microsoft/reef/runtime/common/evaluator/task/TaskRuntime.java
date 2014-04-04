/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.evaluator.task;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.HeartBeatManager;
import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.events.CloseEvent;
import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.reef.task.events.SuspendEvent;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The resourcemanager environment for a Task.
 */
@Private
@EvaluatorSide
public final class TaskRuntime extends Thread {

  private final static Logger LOG = Logger.getLogger(TaskRuntime.class.getName());

  /// User supplied Task code
  private final Task task;
  private final InjectionFuture<EventHandler<CloseEvent>> f_closeHandler;
  private final InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler;
  private final InjectionFuture<EventHandler<DriverMessage>> f_messageHandler;

  // The memento given by the task configuration
  private final Optional<byte[]> memento;

  // Heart beat manager to trigger on heartbeats.
  private final HeartBeatManager heartBeatManager;

  private final TaskStatus currentStatus;

  // TODO: Document
  @Inject
  private TaskRuntime(final HeartBeatManager heartBeatManager,
                      final Task task,
                      final TaskStatus currentStatus,
                      final @Parameter(TaskConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
                      final @Parameter(TaskConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
                      final @Parameter(TaskConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler) {
    this(heartBeatManager, task, currentStatus, f_closeHandler, f_suspendHandler, f_messageHandler, null);
  }

  // TODO: Document
  @Inject
  private TaskRuntime(final HeartBeatManager heartBeatManager,
                      final Task task,
                      final TaskStatus currentStatus,
                      final @Parameter(TaskConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
                      final @Parameter(TaskConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
                      final @Parameter(TaskConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler,
                      final @Parameter(TaskConfigurationOptions.Memento.class) String memento) {
    this.heartBeatManager = heartBeatManager;
    this.task = task;
    this.memento = null == memento ? Optional.<byte[]>empty() :
        Optional.of(DatatypeConverter.parseBase64Binary(memento));

    this.f_closeHandler = f_closeHandler;
    this.f_suspendHandler = f_suspendHandler;
    this.f_messageHandler = f_messageHandler;

    this.currentStatus = currentStatus;

  }

  // TODO: Document
  public void initialize() {
    this.currentStatus.setRunning();
  }

  /**
   * Run the task: Fire TaskStart, call Task.call(), fire TaskStop.
   */
  @Override
  public void run() {
    try {
      LOG.log(Level.FINEST, "call task");
      if (this.currentStatus.isNotRunning()) {
        throw new RuntimeException("TaskRuntime not initialized!");
      }

      final byte[] result;
      if (this.memento.isPresent()) {
        result = this.task.call(this.memento.get());
      } else {
        result = this.task.call(null);
      }

      synchronized (this.heartBeatManager) {
        LOG.log(Level.FINEST, "task call finished");
        this.currentStatus.setResult(result);
      }

    } catch (final InterruptedException e) {
      synchronized (this.heartBeatManager) {
        LOG.log(Level.WARNING, "Killed the Task", e);
        this.currentStatus.setKilled();
      }
    } catch (final Throwable throwable) {
      synchronized (this.heartBeatManager) {
        this.currentStatus.setException(throwable);
      }
    }
  }

  /**
   * Called by heartbeat manager
   *
   * @return current TaskStatusProto
   */
  public final ReefServiceProtos.TaskStatusProto getStatusProto() {
    return this.currentStatus.toProto();
  }

  // TODO: Document
  public final boolean hasEnded() {
    return this.currentStatus.hasEnded();
  }

  /**
   * @return the ID of the task.
   */
  public String getTaskId() {
    return this.currentStatus.getTaskId();
  }

  /**
   * Close the Task. This calls the configured close handler.
   *
   * @param message the optional message for the close handler or null if there none.
   */
  public final void close(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to close a task that is in state: '{0}'. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.f_closeHandler.get().onNext(new CloseEventImpl(message));
          this.currentStatus.setCloseRequested();
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new TaskClientCodeException(
              this.getTaskId(), this.getContextID(), "Error during close().", throwable));
        }
      }
    }
  }

  /**
   * Suspend the Task.  This calls the configured suspend handler.
   *
   * @param message the optional message for the suspend handler or null if there none.
   */
  public final void suspend(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to suspend a task that is in state: '{0}'. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.f_suspendHandler.get().onNext(new SuspendEventImpl(message));
          this.currentStatus.setSuspendRequested();
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new TaskClientCodeException(
              this.getTaskId(), this.getContextID(), "Error during suspend().", throwable));
        }
      }
    }
  }

  /**
   * Deliver a message to the Task. This calls into the user supplied message handler.
   *
   * @param message the message to be delivered.
   */
  public final void deliver(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to send a message to a task that is in state: '{0}'. Ignoring.",
            this.currentStatus.getState());
      } else {
        try {
          this.f_messageHandler.get().onNext(new DriverMessageImpl(message));
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new TaskClientCodeException(
              this.getTaskId(), this.getContextID(), "Error during message delivery.", throwable));
        }
      }
    }
  }

  final String getContextID() {
    return this.currentStatus.getContextId();
  }
}
