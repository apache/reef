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
 * DriverSide representation of EvaluatorStatusProto.
 */
@DriverSide
@Private
public final class EvaluatorStatusPOJO {

  private final String evaluatorID;
  private final byte[] evaluatorIdBytes;
  private final State evaluatorState;
  private final byte[] errorBytes;


  public EvaluatorStatusPOJO(final ReefServiceProtos.EvaluatorStatusProto proto) {

    evaluatorID = proto.getEvaluatorId();
    evaluatorIdBytes = proto.getEvaluatorIdBytes().toByteArray();
    evaluatorState = proto.hasState()? getStateFromProto(proto.getState()) : null;
    errorBytes = proto.hasError() ? proto.getError().toByteArray() : null;

  }

  /**
   * @return true, if an evaluator has thrown an exception and sent it to a driver
   */
  public boolean hasError() {
    return null != errorBytes;
  }

  /**
   * @return serialized exception thrown by an evaluator
   */
  public byte[] getError(){
    return errorBytes;
  }

  /**
   * @return current {@link org.apache.reef.runtime.common.driver.evaluator.pojos.State} of a task
   */
  public State getState(){
    return evaluatorState;
  }

  private State getStateFromProto(final org.apache.reef.proto.ReefServiceProtos.State protoState) {

    switch (protoState) {
    case INIT:
      return State.INIT;
    case RUNNING:
      return State.RUNNING;
    case DONE:
      return State.DONE;
    case SUSPEND:
      return State.SUSPEND;
    case FAILED:
      return State.FAILED;
    case KILLED:
      return State.KILLED;
    default:
      throw new IllegalStateException("Unknown state " + protoState + " in EvaluatorStatusProto");
    }
  }

}
