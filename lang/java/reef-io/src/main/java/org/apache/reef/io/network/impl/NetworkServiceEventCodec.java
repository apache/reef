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
package org.apache.reef.io.network.impl;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.io.network.avro.AvroNetworkServiceEvent;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * NetworkServiceEvent codec using avro
 */
public final class NetworkServiceEventCodec implements Codec<NetworkServiceEvent> {

  private final NetworkPreconfiguredMap preconfiguredMap;

  @Inject
  public NetworkServiceEventCodec(final NetworkPreconfiguredMap preconfiguredMap) {
    this.preconfiguredMap = preconfiguredMap;
  }

  @Override
  public NetworkServiceEvent decode(byte[] data) {
    final AvroNetworkServiceEvent avroNetworkEvent;
    DatumReader<AvroNetworkServiceEvent> reader = new SpecificDatumReader<>(AvroNetworkServiceEvent.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    try {
      avroNetworkEvent = reader.read(null , decoder);
    } catch (IOException e) {
      throw new NetworkRuntimeException(e);
    }

    final Codec<?> codec = preconfiguredMap.getCodec(avroNetworkEvent.getClassNameCode());
    final List<ByteBuffer> byteBufferList = avroNetworkEvent.getDataList();
    final List dataList = new ArrayList(byteBufferList.size());
    for (ByteBuffer byteBuffer : avroNetworkEvent.getDataList()) {
      dataList.add(codec.decode(byteBuffer.array()));
    }

    final String remoteId;
    if (avroNetworkEvent.getRemoteId() == null) {
      remoteId = null;
    } else {
      remoteId  = avroNetworkEvent.getRemoteId().toString();
    }

    return new NetworkServiceEvent(
        avroNetworkEvent.getClassNameCode(),
        dataList,
        remoteId,
        null
    );
  }

  @Override
  public byte[] encode(NetworkServiceEvent obj) {
    final Codec codec = preconfiguredMap.getCodec(obj.getEventClassNameCode());
    final List dataList = obj.getDataList();
    final List<ByteBuffer> byteBufferList = new ArrayList<>(dataList.size());
    for (int i = 0; i < dataList.size(); i++) {
      byteBufferList.add(ByteBuffer.wrap(codec.encode(dataList.get(i))));
    }

    final AvroNetworkServiceEvent event = AvroNetworkServiceEvent.newBuilder()
        .setClassNameCode(obj.getEventClassNameCode())
        .setDataList(byteBufferList)
        .setRemoteId(obj.getRemoteId())
        .build();

    final byte[] bytes;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      DatumWriter<AvroNetworkServiceEvent> writer = new SpecificDatumWriter<>(AvroNetworkServiceEvent.class);
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
      writer.write(event, encoder);
      encoder.flush();
      bytes = bos.toByteArray();
    } catch (IOException e) {
      throw new NetworkRuntimeException(e);
    }

    return bytes;
  }
}
