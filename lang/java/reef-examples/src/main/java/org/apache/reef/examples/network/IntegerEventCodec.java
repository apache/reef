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
package org.apache.reef.examples.network;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Codec for IntegerEvent
 */
public final class IntegerEventCodec implements Codec<IntegerEvent> {

  @Inject
  public IntegerEventCodec(){

  }

  @Override
  public IntegerEvent decode(byte[] data) {
    return new IntegerEvent(ByteBuffer.wrap(data).getInt());
  }

  @Override
  public byte[] encode(IntegerEvent obj) {
    return ByteBuffer.allocate(4).putInt(obj.getInt()).array();
  }
}
