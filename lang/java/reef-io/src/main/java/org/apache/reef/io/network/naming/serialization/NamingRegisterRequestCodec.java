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
package org.apache.reef.io.network.naming.serialization;

import org.apache.reef.io.network.naming.NameAssignmentTuple;
import org.apache.reef.io.network.naming.avro.AvroNamingRegisterRequest;
import org.apache.reef.io.network.naming.exception.NamingRuntimeException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

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
    final AvroNamingRegisterRequest result = AvroNamingRegisterRequest.newBuilder()
        .setId(obj.getNameAssignment().getIdentifier().toString())
        .setHost(obj.getNameAssignment().getAddress().getHostName())
        .setPort(obj.getNameAssignment().getAddress().getPort())
        .build();
    return AvroUtils.toBytes(result, AvroNamingRegisterRequest.class);
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
    final AvroNamingRegisterRequest avroNamingRegisterRequest = AvroUtils.fromBytes(buf, AvroNamingRegisterRequest.class);
    return new NamingRegisterRequest(
        new NameAssignmentTuple(factory.getNewInstance(avroNamingRegisterRequest.getId().toString()),
            new InetSocketAddress(avroNamingRegisterRequest.getHost().toString(), avroNamingRegisterRequest.getPort()))
    );
  }
}