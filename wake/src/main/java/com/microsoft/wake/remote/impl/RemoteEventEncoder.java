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
package com.microsoft.wake.remote.impl;

import com.microsoft.wake.remote.proto.WakeRemoteProtos.WakeMessagePBuf;

import com.google.protobuf.ByteString;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * Remote event encoder using the WakeMessage protocol buffer
 * 
 * @param <T> type
 */
public class RemoteEventEncoder<T> implements Encoder<RemoteEvent<T>>{

  private final Encoder<T> encoder;
  
  /**
   * Constructs a remote event encoder
   * 
   * @param encoder the encoder of the event
   */
  public RemoteEventEncoder(Encoder<T> encoder) {
    this.encoder = encoder;
  }
  
  /**
   * Encodes the remote event object to bytes
   * 
   * @param obj the remote event
   * @return bytes
   * @throws RemoteRuntimeException
   */
  @Override
  public byte[] encode(RemoteEvent<T> obj) {
    if (obj.getSink() == null)  
      throw new RemoteRuntimeException("Sink stage is null");
    if (obj.getEvent() == null)
      throw new RemoteRuntimeException("Event is null");

    WakeMessagePBuf.Builder builder = WakeMessagePBuf.newBuilder();
    String source = obj.getSource() == null ? "" : obj.getSource();
    builder.setSource(source);
    builder.setSink(obj.getSink());
    builder.setSeq(obj.getSeq());
    builder.setData(ByteString.copyFrom(encoder.encode(obj.getEvent())));

    return builder.build().toByteArray();
  }

}
