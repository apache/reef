/**
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
package org.apache.reef.io.network.group.impl.config;

import org.apache.reef.io.network.group.api.config.OperatorSpec;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.serialization.Codec;

/**
 * The specification for the gather operator
 */
public class GatherOperatorSpec implements OperatorSpec {

  private final String receiverId;

  /**
   * Codec to be used to serialize data
   */
  private final Class<? extends Codec> dataCodecClass;

  public GatherOperatorSpec(final String receiverId,
                            final Class<? extends Codec> dataCodecClass) {
    super();
    this.receiverId = receiverId;
    this.dataCodecClass = dataCodecClass;
  }

  public String getReceiverId() {
    return receiverId;
  }

  @Override
  public Class<? extends Codec> getDataCodecClass() {
    return dataCodecClass;
  }

  @Override
  public String toString() {
    return "Gather Operator Spec: [receiver=" + receiverId + "] [dataCodecClass= " + Utils.simpleName(dataCodecClass)
        + "]";
  }

  public static Builder newBuilder() {
    return new GatherOperatorSpec.Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<GatherOperatorSpec> {
    private String receiverId;

    private Class<? extends Codec> dataCodecClass;

    public Builder setReceiverId(final String receiverId) {
      this.receiverId = receiverId;
      return this;
    }

    public Builder setDataCodecClass(final Class<? extends Codec> codecClazz) {
      this.dataCodecClass = codecClazz;
      return this;
    }

    @Override
    public GatherOperatorSpec build() {
      return new GatherOperatorSpec(receiverId, dataCodecClass);
    }
  }
}
