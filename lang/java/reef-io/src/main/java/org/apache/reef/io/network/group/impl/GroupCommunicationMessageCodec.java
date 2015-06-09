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
package org.apache.reef.io.network.group.impl;


import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for {@link org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage}.
 */
public class GroupCommunicationMessageCodec implements StreamingCodec<GroupCommunicationMessage> {

  @Inject
  public GroupCommunicationMessageCodec() {
    // Intentionally Blank
  }

  @Override
  public GroupCommunicationMessage decode(final byte[] data) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        return decodeFromStream(dais);
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  @Override
  public GroupCommunicationMessage decodeFromStream(final DataInputStream stream) {
    try {
      final String groupName = stream.readUTF();
      final String operName = stream.readUTF();
      final Type msgType = Type.valueOf(stream.readInt());
      final String from = stream.readUTF();
      final int srcVersion = stream.readInt();
      final String to = stream.readUTF();
      final int dstVersion = stream.readInt();
      final byte[][] gcmData = new byte[stream.readInt()][];
      for (int i = 0; i < gcmData.length; i++) {
        gcmData[i] = new byte[stream.readInt()];
        stream.readFully(gcmData[i]);
      }
      return new GroupCommunicationMessage(
          groupName,
          operName,
          msgType,
          from,
          srcVersion,
          to,
          dstVersion,
          gcmData);
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  @Override
  public byte[] encode(final GroupCommunicationMessage msg) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(msg, daos);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  @Override
  public void encodeToStream(final GroupCommunicationMessage msg, final DataOutputStream stream) {
    try {
      stream.writeUTF(msg.getGroupname());
      stream.writeUTF(msg.getOperatorname());
      stream.writeInt(msg.getType().getNumber());
      stream.writeUTF(msg.getSrcid());
      stream.writeInt(msg.getSrcVersion());
      stream.writeUTF(msg.getDestid());
      stream.writeInt(msg.getVersion());
      stream.writeInt(msg.getMsgsCount());
      for (final byte[] b : msg.getData()) {
        stream.writeInt(b.length);
        stream.write(b);
      }
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

}
