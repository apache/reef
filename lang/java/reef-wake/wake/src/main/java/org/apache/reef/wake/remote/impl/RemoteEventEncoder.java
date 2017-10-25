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
package org.apache.reef.wake.remote.impl;

import com.google.protobuf.ByteString;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.proto.WakeRemoteProtos.WakeMessagePBuf;

/**
 * Remote event encoder using the WakeMessage protocol buffer.
 *
 * @param <T> type
 */
public class RemoteEventEncoder<T> implements Encoder<RemoteEvent<T>> {

  private final Encoder<T> encoder;

  /**
   * Constructs a remote event encoder.
   *
   * @param encoder the encoder of the event
   */
  public RemoteEventEncoder(final Encoder<T> encoder) {
    this.encoder = encoder;
  }

  /**
   * Encodes the remote event object to bytes.
   *
   * @param obj the remote event
   * @return bytes
   * @throws RemoteRuntimeException
   */
  @Override
  public byte[] encode(final RemoteEvent<T> obj) {
    if (obj.getEvent() == null) {
      throw new RemoteRuntimeException("Event is null");
    }

    final WakeMessagePBuf.Builder builder = WakeMessagePBuf.newBuilder();
    builder.setSeq(obj.getSeq());
    builder.setData(ByteString.copyFrom(encoder.encode(obj.getEvent())));

    return builder.build().toByteArray();
  }

  @Override
  public String toString() {
    return String.format("RemoteEventEncoder: { encoder: %s }", this.encoder);
  }
}
