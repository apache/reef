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
package org.apache.reef.io.network.shuffle.task.operator;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.List;

/**
 *
 */
public interface TupleOperator<K, V> {

  String getGroupingName();

  GroupingDescription<K, V> getGroupingDescription();

  GroupingStrategy<K> getGroupingStrategy();

  List<String> getSelectedReceiverIdList(K key);

  void waitForGroupingSetup();

  void registerControlMessageHandler(EventHandler<Message<ShuffleControlMessage>> messageHandler);

  void registerControlLinkListener(LinkListener<Message<ShuffleControlMessage>> linkListener);

  void sendControlMessage(final String destId, final int code, final byte[][] data);
}
