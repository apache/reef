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

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.wake.remote.Decoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * Remote event decoder using the WakeMessage protocol buffer
 * 
 * @param <T> type
 */
public class RemoteEventDecoder<T> implements Decoder<RemoteEvent<T>> {

  private final Decoder<T> decoder;
  
  /**
   * Constructs a remote event decoder
   * 
   * @param decoder the decoder of the event
   */
  public RemoteEventDecoder(Decoder<T> decoder) {
    this.decoder = decoder;
  }

  /**
   * Decodes a remote event from the byte array data
   * 
   * @param data the byte array data
   * @return a remote event object
   * @throws RemoteRuntimeException
   */
  @Override
  public RemoteEvent<T> decode(byte[] data) {
    WakeMessagePBuf pbuf;
    try {
      pbuf = WakeMessagePBuf.parseFrom(data);
      return new RemoteEvent<T>(null, null, pbuf.getSource(), pbuf.getSink(), pbuf.getSeq(), decoder.decode(pbuf.getData().toByteArray()));
    } catch (InvalidProtocolBufferException e) {
      throw new RemoteRuntimeException(e);
    }
  }

}
