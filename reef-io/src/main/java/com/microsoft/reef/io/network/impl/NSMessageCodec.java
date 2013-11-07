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
package com.microsoft.reef.io.network.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.reef.io.network.exception.NetworkRuntimeException;
import com.microsoft.reef.io.network.proto.ReefNetworkServiceProtos.NSMessagePBuf;
import com.microsoft.reef.io.network.proto.ReefNetworkServiceProtos.NSRecordPBuf;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import java.util.ArrayList;
import java.util.List;

/**
 * Network service message codec
 *
 * @param <T> type
 */
public class NSMessageCodec<T> implements Codec<NSMessage<T>> {

  private final Codec<T> codec;
  private final IdentifierFactory factory;

  /**
   * Constructs a network service message codec
   *
   * @param codec   a codec
   * @param factory an identifier factory
   */
  public NSMessageCodec(Codec<T> codec, IdentifierFactory factory) {
    this.codec = codec;
    this.factory = factory;
  }

  /**
   * Encodes a network service message to bytes
   *
   * @param obj a message
   * @return bytes
   */
  @Override
  public byte[] encode(NSMessage<T> obj) {
    NSMessagePBuf.Builder pbuf = NSMessagePBuf.newBuilder();
    pbuf.setSrcid(obj.getSrcId().toString());
    pbuf.setDestid(obj.getDestId().toString());
    for (T rec : obj.getData()) {
      NSRecordPBuf.Builder rbuf = NSRecordPBuf.newBuilder();
      rbuf.setData(ByteString.copyFrom(codec.encode(rec)));
      pbuf.addMsgs(rbuf);
    }
    return pbuf.build().toByteArray();
  }

  /**
   * Decodes a network service message from bytes
   *
   * @param buf bytes
   * @return a message
   */
  @Override
  public NSMessage<T> decode(byte[] buf) {
    NSMessagePBuf pbuf;
    try {
      pbuf = NSMessagePBuf.parseFrom(buf);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new NetworkRuntimeException(e);
    }
    List<T> list = new ArrayList<T>();
    for (NSRecordPBuf rbuf : pbuf.getMsgsList()) {
      list.add(codec.decode(rbuf.getData().toByteArray()));
    }
    return new NSMessage<T>(factory.getNewInstance(pbuf.getSrcid()),
        factory.getNewInstance(pbuf.getDestid()), list);
  }


}
