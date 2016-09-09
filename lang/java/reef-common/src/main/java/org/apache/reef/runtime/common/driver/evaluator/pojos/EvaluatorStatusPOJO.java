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
    this.evaluatorID = proto.getEvaluatorId();
    this.evaluatorIdBytes = proto.getEvaluatorIdBytes().toByteArray();
    this.evaluatorState = proto.hasState() ? State.fromProto(proto.getState()) : null;
    this.errorBytes = proto.hasError() ? proto.getError().toByteArray() : null;
  }

  /**
   * @return true, if an evaluator has thrown an exception and sent it to a driver.
   */
  public boolean hasError() {
    return null != this.errorBytes;
  }

  /**
   * @return serialized exception thrown by an evaluator.
   */
  public byte[] getError() {
    return this.errorBytes;
  }

  /**
   * @return current state of a task.
   */
  public State getState() {
    return this.evaluatorState;
  }
}
