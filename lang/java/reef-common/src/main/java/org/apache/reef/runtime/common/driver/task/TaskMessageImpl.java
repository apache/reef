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
import org.apache.reef.driver.task.TaskMessage;

/**
 * Implementation of TaskMessage.
 */
@Private
@DriverSide
public final class TaskMessageImpl implements TaskMessage {

  private final byte[] theMessage;
  private final String taskId;
  private final String contextId;
  private final String theMessageSourceId;
  private final long   sequenceNumber;

  public TaskMessageImpl(final byte[] theMessage, final String taskId,
                         final String contextId, final String theMessageSourceId,
                         final long sequenceNumber) {
    this.theMessage = theMessage;
    this.taskId = taskId;
    this.contextId = contextId;
    this.theMessageSourceId = theMessageSourceId;
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public byte[] get() {
    return this.theMessage;
  }

  @Override
  public String getId() {
    return this.taskId;
  }

  @Override
  public String getContextId() {
    return this.contextId;
  }

  @Override
  public String getMessageSourceID() {
    return this.theMessageSourceId;
  }

  @Override
  public long getSequenceNumber() {
    return this.sequenceNumber;
  }
  
}
