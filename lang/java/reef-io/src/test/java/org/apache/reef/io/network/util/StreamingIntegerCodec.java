/*
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
package org.apache.reef.io.network.util;

import org.apache.reef.io.network.impl.StreamingCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class StreamingIntegerCodec implements StreamingCodec<Integer> {

  @Override
  public void encodeToStream(final Integer obj, final DataOutputStream stream) {
    try {
      stream.writeInt(obj);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer decodeFromStream(final DataInputStream stream) {
    try {
      return stream.readInt();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer decode(final byte[] data) {
    return null;
  }

  @Override
  public byte[] encode(final Integer obj) {
    return new byte[0];
  }
}
