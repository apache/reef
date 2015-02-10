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

import org.apache.reef.io.network.naming.avro.AvroNamingLookupRequest;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Naming lookup request codec
 */
public final class NamingLookupRequestCodec implements Codec<NamingLookupRequest> {

  private final IdentifierFactory factory;

  /**
   * Constructs a naming lookup request codec
   *
   * @param factory the identifier factory
   */
  @Inject
  public NamingLookupRequestCodec(final IdentifierFactory factory) {
    this.factory = factory;
  }

  /**
   * Encodes the identifiers to bytes
   *
   * @param obj the naming lookup request
   * @return a byte array
   */
  @Override
  public byte[] encode(final NamingLookupRequest obj) {
    final List<CharSequence> ids = new ArrayList<>();
    for (final Identifier id : obj.getIdentifiers()) {
      ids.add(id.toString());
    }
    return AvroUtils.toBytes(AvroNamingLookupRequest.newBuilder().setIds(ids).build(), AvroNamingLookupRequest.class);
  }

  /**
   * Decodes the bytes to a naming lookup request
   *
   * @param buf the byte array
   * @return a naming lookup request
   */
  @Override
  public NamingLookupRequest decode(final byte[] buf) {
    final AvroNamingLookupRequest req = AvroUtils.fromBytes(buf, AvroNamingLookupRequest.class);

    final List<Identifier> ids = new ArrayList<Identifier>(req.getIds().size());
    for (final CharSequence s : req.getIds()) {
      ids.add(factory.getNewInstance(s.toString()));
    }
    return new NamingLookupRequest(ids);
  }

}
