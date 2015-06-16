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
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for util classes related to Scatter.
 */
public final class ScatterHelperTest {

  /**
   * Test that {@code ScatterHelper.getUniformCounts} functions correctly by giving various sets of inputs.
   */
  @Test
  public void testGetUniformCounts() {
    for (int taskCount = 1; taskCount < 100; taskCount++) {
      final int elementCount = 10000;
      final List<Integer> retVals = ScatterHelper.getUniformCounts(elementCount, taskCount);

      int sum = 0;
      int maxVal = Integer.MIN_VALUE;
      int minVal = Integer.MAX_VALUE;
      int prevVal = Integer.MAX_VALUE;
      for (final int retVal : retVals) {
        sum += retVal;
        maxVal = retVal > maxVal ? retVal : maxVal;
        minVal = retVal < minVal ? retVal : minVal;
        assertTrue(prevVal >= retVal); // monotonic (non-increasing) list check
        prevVal = retVal;
      }
      assertEquals(elementCount, sum); // all elements were considered check
      assertEquals(maxVal - minVal, elementCount % taskCount == 0 ? 0 : 1); // uniform distribution check
    }
  }

  /**
   * Test that {@code ScatterEncoder} and {@code ScatterDecoder} function correctly.
   * Create a small topology of 4 nodes and simulate a scatter operation.
   */
  @Test
  public void testEncodeDecode() {
    final IdentifierFactory ifac = new StringIdentifierFactory();
    final Codec<Integer> codec = new SerializableCodec<>();

    final List<Integer> elements = new LinkedList<>();
    for (int element = 0; element < 400; element++) {
      elements.add(element);
    }

    final List<Integer> counts = new LinkedList<>();
    final List<Identifier> taskOrder = new LinkedList<>();
    for (int index = 0; index < 4; index++) {
      counts.add(100);
      taskOrder.add(ifac.getNewInstance("Task-" + index));
    }

    final TopologySimpleNode rootNode = new TopologySimpleNode("Task-0");
    final TopologySimpleNode childNode1 = new TopologySimpleNode("Task-1");
    final TopologySimpleNode childNode2 = new TopologySimpleNode("Task-2");
    final TopologySimpleNode childNode3 = new TopologySimpleNode("Task-3");
    rootNode.addChild(childNode1);
    rootNode.addChild(childNode2);
    childNode1.addChild(childNode3);

    final CommunicationGroupServiceClient mockCommGroupClient = mock(CommunicationGroupServiceClient.class);
    when(mockCommGroupClient.getTopologySimpleNodeRoot()).thenReturn(rootNode);
    final ScatterEncoder scatterEncoder = new ScatterEncoder(mockCommGroupClient);
    final ScatterDecoder scatterDecoder = new ScatterDecoder();

    final Map<String, byte[]> encodedDataMap = scatterEncoder.encode(elements, counts, taskOrder, codec);

    // check msg correctness for childNode1 (Task-1)
    ScatterData scatterData = scatterDecoder.decode(encodedDataMap.get(childNode1.getTaskId()));
    for (int index = 0; index < 100; index++) {
      assertTrue(index + 100 == codec.decode(scatterData.getMyData()[index]));
    }
    assertTrue(scatterData.getChildrenData().containsKey("Task-3"));
    assertEquals(scatterData.getChildrenData().size(), 1);

    // check msg correctness for childNode2 (Task-2)
    scatterData = scatterDecoder.decode(encodedDataMap.get(childNode2.getTaskId()));
    for (int index = 0; index < 100; index++) {
      assertTrue(index + 200 == codec.decode(scatterData.getMyData()[index]));
    }
    assertTrue(scatterData.getChildrenData().isEmpty());
  }
}
