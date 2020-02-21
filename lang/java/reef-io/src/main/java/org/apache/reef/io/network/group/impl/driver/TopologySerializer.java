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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.api.driver.TaskNode;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class for encoding a Topology into a byte array and vice versa.
 */
public final class TopologySerializer {

  /**
   * Shouldn't be instantiated.
   */
  private TopologySerializer() {
  }

  /**
   * Recursively encode TaskNodes of a Topology into a byte array.
   *
   * @param root the root node of the subtree to encode
   * @return encoded byte array
   */
  public static byte[] encode(final TaskNode root) {
    try (ByteArrayOutputStream bstream = new ByteArrayOutputStream();
         DataOutputStream dstream = new DataOutputStream(bstream)) {
      encodeHelper(dstream, root);
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException("Exception while encoding topology of " + root.getTaskId(), e);
    }
  }

  private static void encodeHelper(final DataOutputStream dstream,
                                   final TaskNode node) throws IOException {
    dstream.writeUTF(node.getTaskId());
    dstream.writeInt(node.getNumberOfChildren());
    for (final TaskNode child : node.getChildren()) {
      encodeHelper(dstream, child);
    }
  }

  /**
   * Recursively translate a byte array into a TopologySimpleNode and a list of task Identifiers.
   *
   * @param data encoded byte array
   * @param ifac IdentifierFactory needed to generate Identifiers from String Ids
   * @return decoded TopologySimpleNode and a lexicographically sorted list of task Identifiers
   */
  public static Pair<TopologySimpleNode, List<Identifier>> decode(
      final byte[] data,
      final IdentifierFactory ifac) {

    try (DataInputStream dstream = new DataInputStream(new ByteArrayInputStream(data))) {
      final List<Identifier> activeSlaveTasks = new LinkedList<>();
      final TopologySimpleNode retNode = decodeHelper(dstream, activeSlaveTasks, ifac);
      return new Pair<>(retNode, activeSlaveTasks);

    } catch (final IOException e) {
      throw new RuntimeException("Exception while decoding message", e);
    }
  }

  private static TopologySimpleNode decodeHelper(
      final DataInputStream dstream,
      final List<Identifier> activeSlaveTasks,
      final IdentifierFactory ifac) throws IOException {
    final String taskId = dstream.readUTF();
    activeSlaveTasks.add(ifac.getNewInstance(taskId));
    final TopologySimpleNode retNode = new TopologySimpleNode(taskId);

    final int children = dstream.readInt();
    for (int index = 0; index < children; index++) {
      final TopologySimpleNode child = decodeHelper(dstream, activeSlaveTasks, ifac);
      retNode.addChild(child);
    }

    return retNode;
  }
}
