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
package org.apache.reef.io.network.group.impl.utils;

import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.impl.driver.TopologySimpleNode;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode messages for a scatter operation, which can be decoded by {@code ScatterDecoder}.
 */
public final class ScatterEncoder {

  private final CommunicationGroupServiceClient commGroupClient;

  @Inject
  ScatterEncoder(final CommunicationGroupServiceClient commGroupClient) {
    this.commGroupClient = commGroupClient;
  }

  public <T> Map<String, byte[]> encode(final List<T> elements,
                                        final List<Integer> counts,
                                        final List<? extends Identifier> taskOrder,
                                        final Codec<T> dataCodec) {

    // first assign data to all tasks
    final Map<String, byte[]> taskIdToBytes = encodeAndDistributeElements(elements, counts, taskOrder, dataCodec);
    // then organize the data so that a node keeps its own data as well as its descendants' data
    final Map<String, byte[]> childIdToBytes = new HashMap<>();

    for (final TopologySimpleNode node : commGroupClient.getTopologySimpleNodeRoot().getChildren()) {
      childIdToBytes.put(node.getTaskId(), encodeScatterMsgForNode(node, taskIdToBytes));
    }
    return childIdToBytes;
  }

  /**
   * Compute a single byte array message for a node and its children.
   * Using {@code taskIdToBytes}, we pack all messages for a
   * {@code TopologySimpleNode} and its children into a single byte array.
   *
   * @param node the target TopologySimpleNode to generate a message for
   * @param taskIdToBytes map containing byte array of encoded data for individual Tasks
   * @return single byte array message
   */
  private byte[] encodeScatterMsgForNode(final TopologySimpleNode node,
                                         final Map<String, byte[]> taskIdToBytes) {

    try (ByteArrayOutputStream bstream = new ByteArrayOutputStream();
         DataOutputStream dstream = new DataOutputStream(bstream)) {

      // first write the node's encoded data
      final String taskId = node.getTaskId();
      if (taskIdToBytes.containsKey(taskId)) {
        dstream.write(taskIdToBytes.get(node.getTaskId()));

      } else {
        // in case mapOfTaskToBytes does not contain this node's id, write an empty
        // message (zero elements)
        dstream.writeInt(0);
      }

      // and then write its children's identifiers and their encoded data
      for (final TopologySimpleNode child : node.getChildren()) {
        dstream.writeUTF(child.getTaskId());
        final byte[] childData = encodeScatterMsgForNode(child, taskIdToBytes);
        dstream.writeInt(childData.length);
        dstream.write(childData);
      }

      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }

  /**
   * Encode elements into byte arrays, and distribute them across Tasks indicated by Identifiers.
   * Note that elements are distributed in the exact order specified in
   * {@code elements} and not in a round-robin fashion.
   * For example, (1, 2, 3, 4) uniformly distributed to (task1, task2, task3) would be
   * {task1: (1, 2), task2: (3), task3: (4)}.
   *
   * @param elements list of data elements to encode
   * @param counts list of numbers specifying how many elements each Task should receive
   * @param taskOrder list of Identifiers indicating Task Ids
   * @param codec class for encoding data
   * @param <T> type of data
   * @return byte representation of a map of identifiers to encoded data
   */
  private <T> Map<String, byte[]> encodeAndDistributeElements(final List<T> elements,
                                                              final List<Integer> counts,
                                                              final List<? extends Identifier> taskOrder,
                                                              final Codec<T> codec) {
    final Map<String, byte[]> taskIdToBytes = new HashMap<>();

    int elementsIndex = 0;
    for (int taskOrderIndex = 0; taskOrderIndex < taskOrder.size(); taskOrderIndex++) {
      final int elementCount = counts.get(taskOrderIndex);

      try (ByteArrayOutputStream bstream = new ByteArrayOutputStream();
           DataOutputStream dstream = new DataOutputStream(bstream)) {

        dstream.writeInt(elementCount);
        for (final T element : elements.subList(elementsIndex, elementsIndex + elementCount)) {
          final byte[] encodedElement = codec.encode(element);
          dstream.writeInt(encodedElement.length);
          dstream.write(encodedElement);
        }
        taskIdToBytes.put(taskOrder.get(taskOrderIndex).toString(), bstream.toByteArray());

      } catch (final IOException e) {
        throw new RuntimeException("IOException",  e);
      }

      elementsIndex += elementCount;
    }

    return taskIdToBytes;
  }
}
