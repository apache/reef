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
package org.apache.reef.io.network.impl;

import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * NSMessage codec.
 *
 */
final class NSMessageCodec implements Codec<NSMessage> {

  private final IdentifierFactory factory;
  private final Map<String, NSConnectionFactory> connectionFactoryMap;
  private final Map<String, Boolean> isStreamingCodecMap;

  /**
   * Constructs a network message codec.
   */
  NSMessageCodec(
      final IdentifierFactory factory,
      final Map<String, NSConnectionFactory> connectionFactoryMap,
      final Map<String, Boolean> isStreamingCodecMap) {

    this.factory = factory;
    this.connectionFactoryMap = connectionFactoryMap;
    this.isStreamingCodecMap = isStreamingCodecMap;
  }

  /**
   * Encodes a network service message to bytes.
   * @param obj a message
   * @return bytes
   */
  @Override
  public byte[] encode(final NSMessage obj) {
    final Codec codec = connectionFactoryMap.get(obj.getConnectionFactoryId()).getCodec();
    final Boolean isStreamingCodec = isStreamingCodecMap.get(obj.getConnectionFactoryId());

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {
        daos.writeUTF(obj.getConnectionFactoryId());
        daos.writeUTF(obj.getSrcId().toString());
        daos.writeUTF(obj.getDestId().toString());
        daos.writeInt(obj.getData().size());

        if (isStreamingCodec) {
          for (final Object rec : obj.getData()) {
            ((StreamingCodec) codec).encodeToStream(rec, daos);
          }
        } else {
          final Iterable dataList = obj.getData();
          for (Object message : dataList) {
            byte[] bytes = codec.encode(message);
            daos.writeInt(bytes.length);
            daos.write(bytes);
          }
        }
        return baos.toByteArray();
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  /**
   * Decodes a network service message from bytes.
   *
   * @param data bytes
   * @return a message
   */
  @Override
  public NSMessage decode(final byte[] data) {

    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final String connectionFactoryId = dais.readUTF();
        final Identifier srcId = factory.getNewInstance(dais.readUTF());
        final Identifier destId = factory.getNewInstance(dais.readUTF());
        final int size = dais.readInt();
        final List list = new ArrayList(size);
        final boolean isStreamingCodec = isStreamingCodecMap.get(connectionFactoryId);
        final Codec codec = connectionFactoryMap.get(connectionFactoryId).getCodec();

        if (isStreamingCodec) {
          for (int i = 0; i < size; i++) {
            list.add(((StreamingCodec) codec).decodeFromStream(dais));
          }
        } else {
          for (int i = 0; i < size; i++) {
            int byteSize = dais.readInt();
            byte[] bytes = new byte[byteSize];
            dais.read(bytes);
            list.add(codec.decode(bytes));
          }
        }

        return new NSMessage(
            connectionFactoryId,
            srcId,
            destId,
            list
        );
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

}
