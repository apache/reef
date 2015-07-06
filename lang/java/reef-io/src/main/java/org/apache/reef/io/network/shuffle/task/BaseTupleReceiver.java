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
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 *
 */
public class BaseTupleReceiver<K, V> implements ShuffleTupleReceiver<K, V> {

  private final ShuffleTopologyClient topologyClient;
  private final GroupingDescription<K, V> groupingDescription;

  @Inject
  public BaseTupleReceiver(
      final ShuffleTopologyClient topologyClient,
      final GroupingDescription<K, V> groupingDescription) {
    this.groupingDescription = groupingDescription;
    this.topologyClient = topologyClient;
  }

  @Override
  public void registerMessageHandler(final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    topologyClient.registerMessageHandler(getGroupingName(), messageHandler);
  }

  @Override
  public String getGroupingName() {
    return groupingDescription.getGroupingName();
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return groupingDescription;
  }
}
