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
package org.apache.reef.io.network.group;

import org.apache.reef.io.network.group.api.driver.TaskNode;
import org.apache.reef.io.network.group.impl.driver.TaskNodeImpl;
import org.apache.reef.io.network.group.impl.driver.TopologySerializer;
import org.apache.reef.io.network.group.impl.driver.TopologySimpleNode;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TopologySerializerTest {

  @Test
  public void testEncodeDecode() {
    final IdentifierFactory ifac = new StringIdentifierFactory();

    // create a topology: Task-0[Task-1[Task-3], Task-2]
    final TaskNode rootNode = new TaskNodeImpl(null, null, null, "Task-0", null, true);
    final TaskNode childNode1 = new TaskNodeImpl(null, null, null, "Task-1", null, false);
    final TaskNode childNode2 = new TaskNodeImpl(null, null, null, "Task-2", null, false);
    final TaskNode childNode3 = new TaskNodeImpl(null, null, null, "Task-3", null, false);
    rootNode.addChild(childNode1);
    rootNode.addChild(childNode2);
    childNode1.addChild(childNode3);

    final Pair<TopologySimpleNode, List<Identifier>> retPair =
        TopologySerializer.decode(TopologySerializer.encode(rootNode), ifac);

    // check topology is recovered
    assertEquals(retPair.getFirst().getTaskId(), "Task-0");
    for (final TopologySimpleNode child : retPair.getFirst().getChildren()) {
      if (child.getTaskId().equals("Task-1")) {
        for (final TopologySimpleNode childchild : child.getChildren()) {
          assertEquals(childchild.getTaskId(), "Task-3");
        }
      } else {
        assertTrue(child.getTaskId().equals("Task-2"));
      }
    }

    // check identifier list contains [Task-0, Task-1, Task-2, Task-3] and nothing else
    assertTrue(retPair.getSecond().contains(ifac.getNewInstance("Task-0")));
    assertTrue(retPair.getSecond().contains(ifac.getNewInstance("Task-1")));
    assertTrue(retPair.getSecond().contains(ifac.getNewInstance("Task-2")));
    assertTrue(retPair.getSecond().contains(ifac.getNewInstance("Task-3")));
    assertEquals(retPair.getSecond().size(), 4);
  }
}
