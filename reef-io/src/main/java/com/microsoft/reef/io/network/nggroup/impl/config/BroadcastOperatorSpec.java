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

import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.serialization.Codec;


/**
 *
 */
public class BroadcastOperatorSpec implements OperatorSpec {
  private final String senderId;

  /**
   * Codec to be used to serialize data
   */
  private final Class<? extends Codec> dataCodecClass;


  public BroadcastOperatorSpec(String senderId,
                               Class<? extends Codec> dataCodecClass) {
    super();
    this.senderId = senderId;
    this.dataCodecClass = dataCodecClass;
  }

  public String getSenderId() {
    return senderId;
  }

  @Override
  public Class<? extends Codec> getDataCodecClass() {
    return dataCodecClass;
  }

  public static Builder newBuilder() {
    return new BroadcastOperatorSpec.Builder();
  }

  public static class Builder implements com.microsoft.reef.util.Builder<BroadcastOperatorSpec> {
    private String senderId;

    /**
     * Codec to be used to serialize data
     */
    private Class<? extends Codec> dataCodecClass;


    public Builder setSenderId(String senderId) {
      this.senderId = senderId;
      return this;
    }

    public Builder setDataCodecClass(Class<? extends Codec> codecClazz) {
      this.dataCodecClass = codecClazz;
      return this;
    }

    @Override
    public BroadcastOperatorSpec build() {
      return new BroadcastOperatorSpec(senderId, dataCodecClass);
    }
  }

}
