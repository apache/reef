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
import org.apache.reef.proto.ReefServiceProtos.ContextStatusProto.ContextMessageProto;

import java.util.ArrayList;
import java.util.List;

/**
 * DriverSide representation of ContextStatusProto.
 */
@DriverSide
@Private
public final class ContextStatusPOJO {

  private final String contextId;
  private final String parentId;
  private final byte[] errorBytes;
  private final ContextState contextState;
  private final List<ContextMessagePOJO> contextMessages = new ArrayList<>();


  public ContextStatusPOJO(final ReefServiceProtos.ContextStatusProto proto, final long sequenceNumber){

    contextId = proto.getContextId();
    parentId = proto.hasParentId() ? proto.getParentId() : null;
    errorBytes = proto.hasError() ? proto.getError().toByteArray() : null;
    contextState = proto.hasContextState()? getContextStateFromProto(proto.getContextState()) : null;

    for (final ContextMessageProto contextMessageProto : proto.getContextMessageList()) {
      contextMessages.add(new ContextMessagePOJO(contextMessageProto,  sequenceNumber));
    }

  }

  /**
   * @return a list of messages sent by a context
   */
  public List<ContextMessagePOJO> getContextMessageList() {
    return contextMessages;
  }

  /**
   * @return the ID of a context
   */
  public String getContextId() {
    return contextId;
  }

  /**
   * @return the {@link org.apache.reef.runtime.common.driver.evaluator.pojos.ContextState} of a context
   */
  public ContextState getContextState() {
    return contextState;
  }

  /**
   * @return true, if a context has thrown an exception and sent it to a driver
   */
  public boolean hasError() {
    return null != errorBytes;
  }

  /**
   * @return serialized exception thrown by a context
   */
  public byte[] getError() {
    return  errorBytes;
  }

  /**
   * @return true, if a context has the parent context
   */
  public boolean hasParentId() {
    return null != parentId;
  }

  /**
   * @return the id of the parent context
   */
  public String getParentId(){
    return parentId;
  }

  private ContextState getContextStateFromProto(
          final org.apache.reef.proto.ReefServiceProtos.ContextStatusProto.State protoState) {

    switch (protoState) {
    case READY:
      return ContextState.READY;
    case DONE:
      return ContextState.DONE;
    case FAIL:
      return ContextState.FAIL;
    default:
      throw new IllegalStateException("Unknown state " + protoState + " in ContextStatusProto");
    }

  }

}
