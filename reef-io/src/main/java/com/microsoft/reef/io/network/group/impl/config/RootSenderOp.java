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
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.remote.Codec;

import java.util.List;

/**
 * This is a type of {@link AsymmetricOpDescription} where the root is the
 * sender - Scatter & Broadcast
 */
public class RootSenderOp extends AsymmetricOpDescription {

  /** Identifier of the sender */
  public final ComparableIdentifier sender;
  /** The receivers */
  public final List<ComparableIdentifier> receivers;

  /**
   * Constructor for fields
   * @param operatorType
   * @param dataCodecClass
   * @param sender
   * @param receivers
   */
  public RootSenderOp(OP_TYPE operatorType,
      Class<? extends Codec<?>> dataCodecClass, ComparableIdentifier sender,
      List<ComparableIdentifier> receivers) {
    super(operatorType, dataCodecClass);
    this.sender = sender;
    this.receivers = receivers;
  }

  /** Builder pattern for fluent description of the operators */
  public static class Builder implements
      com.microsoft.reef.util.Builder<RootSenderOp> {

    private OP_TYPE operatorType;
    private Class<? extends Codec<?>> dataCodecClass;
    private ComparableIdentifier sender;
    private List<ComparableIdentifier> receivers;

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
     * Set the sender or root id
     * 
     * @param sender
     * @return
     */
    public Builder setSender(ComparableIdentifier sender) {
      this.sender = sender;
      return this;
    }

    /**
     * Set the list of ids of receivers
     * 
     * @param receivers
     * @return
     */
    public Builder setReceivers(List<ComparableIdentifier> receivers) {
      this.receivers = receivers;
      return this;
    }

    /**
     * Build the opertaor description
     */
    @Override
    public RootSenderOp build() {
      return new RootSenderOp(operatorType, dataCodecClass, sender, receivers);
    }

  }
}
