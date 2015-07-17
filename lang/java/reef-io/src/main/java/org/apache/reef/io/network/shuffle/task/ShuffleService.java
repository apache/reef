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
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 *
 */
@DefaultImplementation(ShuffleServiceImpl.class)
public interface ShuffleService {

  ShuffleClient getClient(Class<? extends Name<String>> topologyName);

  ShuffleClient getClient(String topologyName);

  void sendControlMessage(String destId, int code, String shuffleName, String groupingName, byte[][] data, byte sourceType, byte sinkType);

  void sendControlMessageToDriver(int code, String shuffleName, String groupingName, byte[][] data, byte sourceType, byte sinkType);

  <K, V> void registerTupleLinkListener(String shuffleName, String groupingName, LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener);

  <K, V> void registerTupleMessageHandler(String shuffleName, String groupingName, EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler);

  void registerSenderGroupingController(String shuffleName, GroupingController groupingController);

  void registerReceiverGroupingController(String shuffleName, GroupingController groupingController);

}
