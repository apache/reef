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
package com.microsoft.reef.io.network.nggroup.impl.config;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.serialization.Codec;

public class ReduceOperatorSpec implements OperatorSpec {

  private final String receiverId;

  /**
   * Codec to be used to serialize data
   */
  private final Class<? extends Codec> dataCodecClass;

  /**
   * The reduce function to be used for operations that do reduction
   */
  public final Class<? extends ReduceFunction> redFuncClass;


  public ReduceOperatorSpec(final String receiverId,
                            final Class<? extends Codec> dataCodecClass,
                            final Class<? extends ReduceFunction> redFuncClass) {
    super();
    this.receiverId = receiverId;
    this.dataCodecClass = dataCodecClass;
    this.redFuncClass = redFuncClass;
  }

  public String getReceiverId() {
    return receiverId;
  }

  /**
   * @return the redFuncClass
   */
  public Class<? extends ReduceFunction> getRedFuncClass() {
    return redFuncClass;
  }

  @Override
  public Class<? extends Codec> getDataCodecClass() {
    return dataCodecClass;
  }

  public static Builder newBuilder() {
    return new ReduceOperatorSpec.Builder();
  }

  public static class Builder implements com.microsoft.reef.util.Builder<ReduceOperatorSpec> {

    private String receiverId;

    /**
     * Codec to be used to serialize data
     */
    private Class<? extends Codec> dataCodecClass;

    private Class<? extends ReduceFunction> redFuncClass;

    public Builder setReceiverId(String receiverId) {
      this.receiverId = receiverId;
      return this;
    }

    public Builder setDataCodecClass(Class<? extends Codec> codecClazz) {
      this.dataCodecClass = codecClazz;
      return this;
    }

    public Builder setReduceFunctionClass(Class<? extends ReduceFunction> redFuncClass) {
      this.redFuncClass = redFuncClass;
      return this;
    }

    @Override
    public ReduceOperatorSpec build() {
      return new ReduceOperatorSpec(receiverId, dataCodecClass, redFuncClass);
    }
  }
}
