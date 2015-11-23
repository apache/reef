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

/**
 *  DriverSide representation of TaskMessageProto.
 */
@DriverSide
@Private
public final class TaskMessagePOJO {

  private final byte[] message;
  private final String sourceId;

  TaskMessagePOJO(final ReefServiceProtos.TaskStatusProto.TaskMessageProto proto){
    message = proto.getMessage().toByteArray();
    sourceId = proto.getSourceId();
  }

  /**
   * @return the serialized message from a task
   */
  public byte[] getMessage() {
    return message;
  }

  /**
   * @return the ID of the TaskMessageSource that sent the message.
   */
  public String getSourceId(){
    return sourceId;
  }
}
