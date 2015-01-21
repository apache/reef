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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.remote.Codec;

/**
 * Codec of the event sent remotely
 *
 * @param <T> type
 */
public class RemoteEventCodec<T> implements Codec<RemoteEvent<T>> {

  private final RemoteEventEncoder<T> encoder;
  private final RemoteEventDecoder<T> decoder;

  /**
   * Constructs a remote event codec
   *
   * @param codec the codec for the event
   */
  public RemoteEventCodec(Codec<T> codec) {
    encoder = new RemoteEventEncoder<T>(codec);
    decoder = new RemoteEventDecoder<T>(codec);
  }

  /**
   * Encodes the remote event object to bytes
   *
   * @param obj the remote event object
   * @returns bytes
   */
  @Override
  public byte[] encode(RemoteEvent<T> obj) {
    return encoder.encode(obj);
  }

  /**
   * Decodes a remote event object from the bytes
   *
   * @param data the byte array
   * @return a remote event object
   */
  @Override
  public RemoteEvent<T> decode(byte[] data) {
    return decoder.decode(data);
  }

}
