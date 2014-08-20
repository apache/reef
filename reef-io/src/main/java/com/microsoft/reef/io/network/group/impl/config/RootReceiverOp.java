/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.impl.config;

import com.microsoft.reef.io.network.group.config.OP_TYPE;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.remote.Codec;

import java.util.List;

/**
 * This is a type of {@link AsymmetricOpDescription} where the root is a
 * receiver - Reduce & Gather
 */
public class RootReceiverOp extends AsymmetricOpDescription {
  /** The id of the recevier */
  public final ComparableIdentifier receiver;
  /** The ids of senders */
  public final List<ComparableIdentifier> senders;
  /** Reduce Function class to be used */
  public final Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * Constructor for fields
   * 
   * @param operatorType
   * @param dataCodecClass
   * @param receiver
   * @param senders
   * @param redFuncClass
   */
  public RootReceiverOp(OP_TYPE operatorType,
      Class<? extends Codec<?>> dataCodecClass, ComparableIdentifier receiver,
      List<ComparableIdentifier> senders,
      Class<? extends ReduceFunction<?>> redFuncClass) {
    super(operatorType, dataCodecClass);
    this.receiver = receiver;
    this.senders = senders;
    this.redFuncClass = redFuncClass;
  }

  /** Builder for fluent description of operators */
  public static class Builder implements
      com.microsoft.reef.util.Builder<RootReceiverOp> {

    private OP_TYPE operatorType;
    private Class<? extends Codec<?>> dataCodecClass;
    private Class<? extends ReduceFunction<?>> redFuncClass;
    private List<ComparableIdentifier> senders;
    private ComparableIdentifier receiver;

    /**
     * Override the operator type which is typically automatically set
     * 
     * @param operatorType
     * @return
     */
    public Builder setOpertaorType(OP_TYPE operatorType) {
      this.operatorType = operatorType;
      return this;
    }

    /**
     * Set the Data Codec class typically inherited from GroupOperators
     * 
     * @param dataCodecClass
     * @return
     */
    public Builder setDataCodecClass(Class<? extends Codec<?>> dataCodecClass) {
      this.dataCodecClass = dataCodecClass;
      return this;
    }

    /**
     * Set the list of ids of senders
     * 
     * @param senders
     * @return
     */
    public Builder setSenders(List<ComparableIdentifier> senders) {
      this.senders = senders;
      return this;
    }

    /**
     * Set the receiver or root id
     * 
     * @param receiver
     * @return
     */
    public Builder setReceiver(ComparableIdentifier receiver) {
      this.receiver = receiver;
      return this;
    }

    /**
     * Set the reduce function class to be used
     * 
     * @param redFuncClass
     * @return
     */
    public Builder setRedFuncClass(
        Class<? extends ReduceFunction<?>> redFuncClass) {
      this.redFuncClass = redFuncClass;
      return this;
    }

    /**
     * Build the opertaor description
     */
    @Override
    public RootReceiverOp build() {
      return new RootReceiverOp(operatorType, dataCodecClass, receiver,
          senders, redFuncClass);
    }

  }

}
