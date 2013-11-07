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
import com.microsoft.reef.io.network.naming.NameAssignmentTuple;
import com.microsoft.reef.io.network.naming.exception.NamingRuntimeException;
import com.microsoft.reef.io.network.proto.ReefNetworkNamingProtos.NamingRegisterRequestPBuf;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import java.net.InetSocketAddress;

/**
 * Naming registration request codec
 */
public class NamingRegisterRequestCodec implements Codec<NamingRegisterRequest> {

  private final IdentifierFactory factory;

  /**
   * Constructs a naming registration request codec
   *
   * @param factory the identifier factory
   */
  public NamingRegisterRequestCodec(IdentifierFactory factory) {
    this.factory = factory;
  }

  /**
   * Encodes the name assignment to bytes
   *
   * @param obj the naming registration request
   * @return a byte array
   */
  @Override
  public byte[] encode(NamingRegisterRequest obj) {
    NamingRegisterRequestPBuf.Builder builder = NamingRegisterRequestPBuf.newBuilder();
    builder.setId(obj.getNameAssignment().getIdentifier().toString());
    builder.setHost(obj.getNameAssignment().getAddress().getHostName());
    builder.setPort(obj.getNameAssignment().getAddress().getPort());
    return builder.build().toByteArray();
  }

  /**
   * Decodes the bytes to a name assignment
   *
   * @param buf the byte array
   * @return a naming registration request
   * @throws NamingRuntimeException
   */
  @Override
  public NamingRegisterRequest decode(byte[] buf) {
    NamingRegisterRequestPBuf pbuf;
    try {
      pbuf = NamingRegisterRequestPBuf.parseFrom(buf);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new NamingRuntimeException(e);
    }
    return new NamingRegisterRequest(new NameAssignmentTuple(factory.getNewInstance(pbuf.getId()),
        new InetSocketAddress(pbuf.getHost(), pbuf.getPort())));
  }

}
