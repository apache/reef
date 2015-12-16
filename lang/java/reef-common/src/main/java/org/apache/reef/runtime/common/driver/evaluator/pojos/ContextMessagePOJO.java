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
import org.apache.reef.io.Numberable;
import org.apache.reef.proto.ReefServiceProtos;

/**
 *  DriverSide representation of ContextMessageProto.
 */
@DriverSide
@Private
public final class ContextMessagePOJO implements Numberable {

  private final byte[] message;
  private final String sourceId;
  private final long sequenceNumber;


  ContextMessagePOJO(final ReefServiceProtos.ContextStatusProto.ContextMessageProto proto, final long sequenceNumber){
    message = proto.getMessage().toByteArray();
    sourceId = proto.getSourceId();
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * @return the serialized message from a context.
   */
  public byte[] getMessage(){
    return message;
  }

  /**
   * @return the ID of the ContextMessageSource that sent the message.
   */
  public String getSourceId(){
    return sourceId;
  }

  /**
   * @return the sequence number of a message
   *
   * {@see ContextMessage#getSequenceNumber()}
   */
  @Override
  public long getSequenceNumber(){
    return this.sequenceNumber;
  }
}
