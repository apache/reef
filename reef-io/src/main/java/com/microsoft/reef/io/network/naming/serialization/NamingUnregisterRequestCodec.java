/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io.network.naming.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.reef.io.network.naming.exception.NamingRuntimeException;
import com.microsoft.reef.io.network.proto.ReefNetworkNamingProtos.NamingUnregisterRequestPBuf;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

/**
 * Naming un-registration request codec
 */
public class NamingUnregisterRequestCodec implements Codec<NamingUnregisterRequest> {

  private final IdentifierFactory factory;

  /**
   * Constructs a naming un-registration request codec
   *
   * @param factory the identifier factory
   */
  public NamingUnregisterRequestCodec(IdentifierFactory factory) {
    this.factory = factory;
  }

  /**
   * Encodes the naming un-registration request to bytes
   *
   * @param obj the naming un-registration request
   * @return a byte array
   */
  @Override
  public byte[] encode(NamingUnregisterRequest obj) {
    NamingUnregisterRequestPBuf.Builder builder = NamingUnregisterRequestPBuf.newBuilder();
    builder.setId(obj.getIdentifier().toString());
    return builder.build().toByteArray();
  }

  /**
   * Decodes the bytes to a naming un-registration request
   *
   * @param buf the byte array
   * @return a naming un-registration request
   * @throws NamingRuntimeException
   */
  @Override
  public NamingUnregisterRequest decode(byte[] buf) {
    NamingUnregisterRequestPBuf pbuf;
    try {
      pbuf = NamingUnregisterRequestPBuf.parseFrom(buf);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new NamingRuntimeException(e);
    }
    return new NamingUnregisterRequest(factory.getNewInstance(pbuf.getId()));
  }

}
