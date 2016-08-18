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

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.proto.EvaluatorRuntimeProtocol.ContextControlProto;
import org.apache.reef.proto.EvaluatorRuntimeProtocol.StopTaskProto;
import org.apache.reef.proto.EvaluatorRuntimeProtocol.SuspendTaskProto;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the RunningTask client interface. It is mainly a helper class
 * that will package up various client method calls into protocol buffers and
 * pass them to its respective EvaluatorManager to deliver to the EvaluatorRuntime.
 */
@Private
@DriverSide
public final class RunningTaskImpl implements RunningTask {

  private static final Logger LOG = Logger.getLogger(RunningTask.class.getName());

  private final EvaluatorManager evaluatorManager;
  private final EvaluatorContext evaluatorContext;
  private final String taskId;
  private final TaskRepresenter taskRepresenter;

  public RunningTaskImpl(final EvaluatorManager evaluatorManager,
                         final String taskId,
                         final EvaluatorContext evaluatorContext,
                         final TaskRepresenter taskRepresenter) {
    LOG.log(Level.FINEST, "INIT: TaskRuntime id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    this.evaluatorManager = evaluatorManager;
    this.evaluatorContext = evaluatorContext;
    this.taskId = taskId;
    this.taskRepresenter = taskRepresenter;
  }


  @Override
  public ActiveContext getActiveContext() {
    return this.evaluatorContext;
  }

  @Override
  public String getId() {
    return this.taskId;
  }

  @Override
  public void send(final byte[] message) {
    LOG.log(Level.FINEST, "MESSAGE: Task id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    final ContextControlProto contextControlProto = ContextControlProto.newBuilder()
        .setTaskMessage(ByteString.copyFrom(message))
        .build();

    this.evaluatorManager.sendContextControlMessage(contextControlProto);
  }

  @Override
  public void close() {
    LOG.log(Level.FINEST, "CLOSE: TaskRuntime id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    if (this.taskRepresenter.isClosable()) {
      final ContextControlProto contextControlProto = ContextControlProto.newBuilder()
          .setStopTask(StopTaskProto.newBuilder().build())
          .build();
      this.evaluatorManager.sendContextControlMessage(contextControlProto);
    } else {
      LOG.log(Level.INFO, "Ignoring call to .close() because the task is no longer RUNNING.");
    }
  }

  @Override
  public void close(final byte[] message) {
    LOG.log(Level.FINEST, "CLOSE: TaskRuntime id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() +
        "] with message.");
    if (this.taskRepresenter.isClosable()) {
      final ContextControlProto contextControlProto = ContextControlProto.newBuilder()
          .setStopTask(StopTaskProto.newBuilder().build())
          .setTaskMessage(ByteString.copyFrom(message))
          .build();
      this.evaluatorManager.sendContextControlMessage(contextControlProto);
    } else {
      LOG.log(Level.INFO, "Ignoring call to .close(byte[] message) because the task is no longer RUNNING "
          + "(see REEF-1503 for an example of scenario in which this happens).");
    }
  }

  @Override
  public void suspend(final byte[] message) {
    LOG.log(Level.FINEST, "SUSPEND: TaskRuntime id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() +
        "] with message.");

    final ContextControlProto contextControlProto = ContextControlProto.newBuilder()
        .setSuspendTask(SuspendTaskProto.newBuilder().build())
        .setTaskMessage(ByteString.copyFrom(message))
        .build();
    this.evaluatorManager.sendContextControlMessage(contextControlProto);
  }

  @Override
  public void suspend() {
    LOG.log(Level.FINEST, "SUSPEND: TaskRuntime id[" + taskId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    final ContextControlProto contextControlProto = ContextControlProto.newBuilder()
        .setSuspendTask(SuspendTaskProto.newBuilder().build())
        .build();
    this.evaluatorManager.sendContextControlMessage(contextControlProto);
  }

  @Override
  public String toString() {
    return "RunningTask{taskId='" + taskId + "'}";
  }

  public TaskRepresenter getTaskRepresenter() {
    return taskRepresenter;
  }
}
