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

package org.apache.reef.runtime.common.driver.evaluator.pojos;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.proto.ReefServiceProtos.TaskStatusProto.TaskMessageProto;

import java.util.ArrayList;
import java.util.List;

/**
 * DriverSide representation of TaskStatusProto.
 */
@DriverSide
@Private
public final class TaskStatusPOJO {

  private final String taskId;
  private final String contextId;
  private final State state;
  private final byte[] result;
  private final List<TaskMessagePOJO> taskMessages = new ArrayList<>();

  public TaskStatusPOJO(final ReefServiceProtos.TaskStatusProto proto, final long sequenceNumber) {

    this.taskId = proto.getTaskId();
    this.contextId = proto.getContextId();
    this.state = proto.hasState() ? State.fromProto(proto.getState()) : null;
    this.result = proto.hasResult() ? proto.getResult().toByteArray() : null;

    for (final TaskMessageProto taskMessageProto : proto.getTaskMessageList()) {
      this.taskMessages.add(new TaskMessagePOJO(taskMessageProto, sequenceNumber));
    }
  }

  /**
   * @return a list of messages sent by a task.
   */
  public List<TaskMessagePOJO> getTaskMessageList() {
    return this.taskMessages;
  }

  /**
   * @return true, if a completed task returned a non-null value in the 'return' statement.
   */
  public boolean hasResult() {
    return null != this.result;
  }

  /**
   * @return serialized result that a completed task returned to the Driver.
   */
  public byte[] getResult() {
    return this.result;
  }

  /**
   * @return the id of a task.
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * @return the id of a context that this task runs within.
   */
  public String getContextId() {
    return this.contextId;
  }

  /**
   * @return current state of a task.
   */
  public State getState() {
    return this.state;
  }
}
