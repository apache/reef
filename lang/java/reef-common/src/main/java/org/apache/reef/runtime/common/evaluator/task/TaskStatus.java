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
package org.apache.reef.runtime.common.evaluator.task;

import com.google.protobuf.ByteString;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.evaluator.HeartBeatManager;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the various states a Task could be in.
 */
public final class TaskStatus {
  private static final Logger LOG = Logger.getLogger(TaskStatus.class.getName());

  private final String taskId;
  private final String contextId;
  private final HeartBeatManager heartBeatManager;
  private final Set<TaskMessageSource> evaluatorMessageSources;
  private final ExceptionCodec exceptionCodec;
  private Optional<Throwable> lastException = Optional.empty();
  private Optional<byte[]> result = Optional.empty();
  private State state = State.PRE_INIT;


  @Inject
  TaskStatus(@Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
             @Parameter(ContextIdentifier.class) final String contextId,
             @Parameter(TaskConfigurationOptions.TaskMessageSources.class)
             final Set<TaskMessageSource> evaluatorMessageSources,
             final HeartBeatManager heartBeatManager,
             final ExceptionCodec exceptionCodec) {
    this.taskId = taskId;
    this.contextId = contextId;
    this.heartBeatManager = heartBeatManager;
    this.evaluatorMessageSources = evaluatorMessageSources;
    this.exceptionCodec = exceptionCodec;
  }

  /**
   * @param from
   * @param to
   * @return true, if the state transition from state 'from' to state 'to' is legal.
   */
  private static boolean isLegal(final State from, final State to) {
    if (from == null) {
      return to == State.INIT;
    }
    switch (from) {
    case PRE_INIT:
      switch (to) {
      case INIT:
        return true;
      default:
        return false;
      }
    case INIT:
      switch (to) {
      case RUNNING:
      case FAILED:
      case KILLED:
      case DONE:
        return true;
      default:
        return false;
      }
    case RUNNING:
      switch (to) {
      case CLOSE_REQUESTED:
      case SUSPEND_REQUESTED:
      case FAILED:
      case KILLED:
      case DONE:
        return true;
      default:
        return false;
      }
    case CLOSE_REQUESTED:
      switch (to) {
      case FAILED:
      case KILLED:
      case DONE:
        return true;
      default:
        return false;
      }
    case SUSPEND_REQUESTED:
      switch (to) {
      case FAILED:
      case KILLED:
      case SUSPENDED:
        return true;
      default:
        return false;
      }

    case FAILED:
    case DONE:
    case KILLED:
      return false;
    default:
      return false;
    }
  }

  public String getTaskId() {
    return this.taskId;
  }

  ReefServiceProtos.TaskStatusProto toProto() {
    this.check();
    final ReefServiceProtos.TaskStatusProto.Builder resultBuilder = ReefServiceProtos.TaskStatusProto.newBuilder()
        .setContextId(this.contextId)
        .setTaskId(this.taskId)
        .setState(this.getProtoState());

    if (this.result.isPresent()) {
      resultBuilder.setResult(ByteString.copyFrom(this.result.get()));
    } else if (this.lastException.isPresent()) {
      final byte[] error = this.exceptionCodec.toBytes(this.lastException.get());
      resultBuilder.setResult(ByteString.copyFrom(error));
    } else if (this.state == State.RUNNING) {
      for (final TaskMessage taskMessage : this.getMessages()) {
        resultBuilder.addTaskMessage(ReefServiceProtos.TaskStatusProto.TaskMessageProto.newBuilder()
            .setSourceId(taskMessage.getMessageSourceID())
            .setMessage(ByteString.copyFrom(taskMessage.get()))
            .build());
      }
    }

    return resultBuilder.build();
  }

  private void check() {
    if (this.result.isPresent() && this.lastException.isPresent()) {
      throw new RuntimeException("Found both an exception and a result. This is unsupported.");
    }
  }

  private ReefServiceProtos.State getProtoState() {
    switch (this.state) {
    case INIT:
      return ReefServiceProtos.State.INIT;
    case CLOSE_REQUESTED:
    case SUSPEND_REQUESTED:
    case RUNNING:
      return ReefServiceProtos.State.RUNNING;
    case DONE:
      return ReefServiceProtos.State.DONE;
    case SUSPENDED:
      return ReefServiceProtos.State.SUSPEND;
    case FAILED:
      return ReefServiceProtos.State.FAILED;
    case KILLED:
      return ReefServiceProtos.State.KILLED;
    default:
      throw new RuntimeException("Unknown state: " + this.state);
    }
  }

  void setException(final Throwable throwable) {
    synchronized (this.heartBeatManager) {
      this.lastException = Optional.of(throwable);
      this.state = State.FAILED;
      this.check();
      this.heartbeat();
    }
  }

  void setResult(final byte[] result) {
    synchronized (this.heartBeatManager) {
      this.result = Optional.ofNullable(result);
      if (this.state == State.RUNNING) {
        this.setState(State.DONE);
      } else if (this.state == State.SUSPEND_REQUESTED) {
        this.setState(State.SUSPENDED);
      } else if (this.state == State.CLOSE_REQUESTED) {
        this.setState(State.DONE);
      }
      this.check();
      this.heartbeat();
    }
  }

  private void heartbeat() {
    this.heartBeatManager.sendTaskStatus(this.toProto());
  }

  /**
   * Sets the state to INIT and informs the driver about it.
   */
  void setInit() {
    LOG.log(Level.FINEST, "Sending Task INIT heartbeat to the Driver.");
    this.setState(State.INIT);
    this.heartbeat();
  }

  /**
   * Sets the state to RUNNING after the handlers for TaskStart have been called.
   */
  void setRunning() {
    this.setState(State.RUNNING);
    this.heartbeat();
  }

  void setCloseRequested() {
    this.setState(State.CLOSE_REQUESTED);
  }

  void setSuspendRequested() {
    this.setState(State.SUSPEND_REQUESTED);
  }

  void setKilled() {
    this.setState(State.KILLED);
    this.heartbeat();
  }

  boolean isRunning() {
    return this.state == State.RUNNING;
  }

  boolean isNotRunning() {
    return this.state != State.RUNNING;
  }

  boolean hasEnded() {
    switch (this.state) {
    case DONE:
    case SUSPENDED:
    case FAILED:
    case KILLED:
      return true;
    default:
      return false;
    }
  }

  State getState() {
    return this.state;
  }

  private void setState(final State state) {
    if (isLegal(this.state, state)) {
      this.state = state;
    } else {
      final String msg = "Illegal state transition from [" + this.state + "] to [" + state + "]";
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }
  }

  String getContextId() {
    return this.contextId;
  }

  /**
   * @return the messages to be sent on the Task's behalf in the next heartbeat.
   */
  private Collection<TaskMessage> getMessages() {
    final List<TaskMessage> messageList = new ArrayList<>(this.evaluatorMessageSources.size());
    for (final TaskMessageSource messageSource : this.evaluatorMessageSources) {
      final Optional<TaskMessage> taskMessageOptional = messageSource.getMessage();
      if (taskMessageOptional.isPresent()) {
        messageList.add(taskMessageOptional.get());
      }
    }
    return messageList;
  }


  enum State {
    PRE_INIT,
    INIT,
    RUNNING,
    CLOSE_REQUESTED,
    SUSPEND_REQUESTED,
    SUSPENDED,
    FAILED,
    DONE,
    KILLED
  }
}
