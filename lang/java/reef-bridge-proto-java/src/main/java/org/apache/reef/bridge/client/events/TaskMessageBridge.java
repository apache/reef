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

package org.apache.reef.bridge.client.events;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.TaskMessage;

/**
 * Task message bridge.
 */
@Private
public final class TaskMessageBridge implements TaskMessage {

  private final String taskId;

  private final String contextId;

  private final String messageSourceId;

  private final long sequenceNumber;

  private final byte[] message;

  public TaskMessageBridge(
      final String taskId,
      final String contextId,
      final String messageSourceId,
      final long sequenceNumber,
      final byte[] message) {
    this.taskId = taskId;
    this.contextId = contextId;
    this.messageSourceId = messageSourceId;
    this.sequenceNumber = sequenceNumber;
    this.message = message;
  }

  @Override
  public byte[] get() {
    return this.message;
  }

  @Override
  public String getId() {
    return this.taskId;
  }

  @Override
  public long getSequenceNumber() {
    return this.sequenceNumber;
  }

  @Override
  public String getContextId() {
    return this.contextId;
  }

  @Override
  public String getMessageSourceID() {
    return this.messageSourceId;
  }
}
