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
import com.microsoft.reef.io.naming.NameAssignment;
import com.microsoft.reef.io.network.naming.NameAssignmentTuple;
import com.microsoft.reef.io.network.naming.exception.NamingRuntimeException;
import com.microsoft.reef.io.network.proto.ReefNetworkNamingProtos.NameAssignmentPBuf;
import com.microsoft.reef.io.network.proto.ReefNetworkNamingProtos.NamingLookupResponsePBuf;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Naming lookup response codec
 */
public class NamingLookupResponseCodec implements Codec<NamingLookupResponse> {

  private final IdentifierFactory factory;
  
  /**
   * Constructs a naming lookup response codec
   * 
   * @param factory the identifier factory
   */
  public NamingLookupResponseCodec(IdentifierFactory factory) {
    this.factory = factory;
  }
  
  /**
   * Encodes name assignments to bytes
   * 
   * @param obj the naming lookup response
   * @return a byte array
   */
  @Override
  public byte[] encode(NamingLookupResponse obj) {
    NamingLookupResponsePBuf.Builder builder = NamingLookupResponsePBuf.newBuilder();
    for (NameAssignment na : obj.getNameAssignments()) {
      builder.addTuples(NameAssignmentPBuf.newBuilder()
          .setId(na.getIdentifier().toString())
          .setHost(na.getAddress().getHostName())
          .setPort(na.getAddress().getPort()));
    }
    return builder.build().toByteArray();
  }

  /**
   * Decodes bytes to an iterable of name assignments
   * 
   * @param buf the byte array
   * @return a naming lookup response
   * @throws NamingRuntimeException
   */
  @Override
  public NamingLookupResponse decode(byte[] buf) {
    NamingLookupResponsePBuf pbuf;
    try {
      pbuf = NamingLookupResponsePBuf.parseFrom(buf);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new NamingRuntimeException(e);
    }
    List<NameAssignment> nas = new ArrayList<NameAssignment>(); 
    for (NameAssignmentPBuf tuple : pbuf.getTuplesList()) {
      nas.add(new NameAssignmentTuple(factory.getNewInstance(tuple.getId()), new InetSocketAddress(tuple.getHost(), tuple.getPort())));
    }
    return new NamingLookupResponse(nas);
  }
 
}
