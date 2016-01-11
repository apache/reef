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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DefaultNetworkMessageCodec implementation.
 * This codec encodes/decodes NetworkConnectionServiceMessageImpl according to the type <T>.
 */
final class NetworkConnectionServiceMessageCodec implements Codec<NetworkConnectionServiceMessage> {
  private static final Logger LOG = Logger.getLogger(NetworkConnectionServiceMessageCodec.class.getName());

  private final IdentifierFactory factory;
  /**
   * Contains entries of (id of connection factory, instance of connection factory).
   */
  private final Map<String, NetworkConnectionFactory> connFactoryMap;
  /**
   * Contains entries of (instance of codec, boolean whether the codec is streaming or not).
   */
  private final ConcurrentMap<Codec, Boolean> isStreamingCodecMap;

  /**
   * Constructs a network connection service message codec.
   */
  NetworkConnectionServiceMessageCodec(
      final IdentifierFactory factory,
      final Map<String, NetworkConnectionFactory> connFactoryMap) {
    this.factory = factory;
    this.connFactoryMap = connFactoryMap;
    this.isStreamingCodecMap = new ConcurrentHashMap<>();
  }

  /**
   * Encodes a network connection service message to bytes.
   * @param obj a message
   * @return bytes
   */
  @Override
  public byte[] encode(final NetworkConnectionServiceMessage obj) {
    final Codec codec = connFactoryMap.get(obj.getConnectionFactoryId()).getCodec();
    Boolean isStreamingCodec = isStreamingCodecMap.get(codec);
    if (isStreamingCodec == null) {
      isStreamingCodec = codec instanceof StreamingCodec;
      isStreamingCodecMap.putIfAbsent(codec, isStreamingCodec);
    }

    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final DataOutputStream daos = new DataOutputStream(baos)) {
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
          for (final Object message : dataList) {
            final byte[] bytes = codec.encode(message);
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
   * Decodes a network connection service message from bytes.
   *
   * @param data bytes
   * @return a message
   */
  @Override
  public NetworkConnectionServiceMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (final DataInputStream dais = new DataInputStream(bais)) {
        final String connFactoryId = dais.readUTF();
        final Identifier srcId = factory.getNewInstance(dais.readUTF());
        final Identifier destId = factory.getNewInstance(dais.readUTF());
        final int size = dais.readInt();
        final List list = new ArrayList(size);
        final Codec codec = connFactoryMap.get(connFactoryId).getCodec();
        Boolean isStreamingCodec = isStreamingCodecMap.get(codec);
        if (isStreamingCodec == null) {
          isStreamingCodec = codec instanceof StreamingCodec;
          isStreamingCodecMap.putIfAbsent(codec, isStreamingCodec);
        }

        if (isStreamingCodec) {
          for (int i = 0; i < size; i++) {
            list.add(((StreamingCodec) codec).decodeFromStream(dais));
          }
        } else {
          for (int i = 0; i < size; i++) {
            final int byteSize = dais.readInt();
            final byte[] bytes = new byte[byteSize];
            if (dais.read(bytes) == -1) {
              LOG.log(Level.FINE, "No data read because end of stream was reached");
            }
            list.add(codec.decode(bytes));
          }
        }

        return new NetworkConnectionServiceMessage(
            connFactoryId,
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