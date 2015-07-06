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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.io.network.shuffle.topology.ShuffleTopologyController;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ShuffleTopologyClient extends ShuffleTopologyController {

  boolean waitForTopologySetup();

  EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler();

  LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener();

  Codec<Tuple> getTupleCodec(String groupingName);

  Map<String, GroupingDescription> getGroupingDescriptionMap();

  Map<String, NodePoolDescription> getNodePoolDescriptionMap();

  Map<String, NodePoolDescription> getSenderPoolDescriptionMap();

  Map<String, NodePoolDescription> getReceiverPoolDescriptionMap();

  <K, V> ShuffleTupleReceiver<K, V> getReceiver(String groupingName);

  <K, V> ShuffleTupleSender<K, V> getSender(String groupingName);

  <K, V> void registerLinkListener(String groupingName, LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener);

  <K, V> void registerMessageHandler(String groupingName, EventHandler<Message<ShuffleTupleMessage<K ,V>>> messageHandler);
}
