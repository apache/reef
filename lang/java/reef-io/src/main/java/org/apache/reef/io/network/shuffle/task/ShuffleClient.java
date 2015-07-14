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
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.ShuffleController;
import org.apache.reef.io.network.shuffle.task.operator.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.operator.TupleSender;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 *
 */
public interface ShuffleClient extends ShuffleController {

  void waitForGroupingSetup(String groupingName);

  void waitForSetup();

  EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler();

  LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener();

  Codec<Tuple> getTupleCodec(String groupingName);

  <K, V> TupleReceiver<K, V> getReceiver(String groupingName);

  <K, V> TupleSender<K, V> getSender(String groupingName);

  <K, V> void registerTupleLinkListener(String groupingName, LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener);

  <K, V> void registerTupleMessageHandler(String groupingName, EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler);

  void registerControlLinkListener(String groupingName, LinkListener<Message<ShuffleControlMessage>> linkListener);

  void registerControlMessageHandler(String groupingName, EventHandler<Message<ShuffleControlMessage>> messageHandler);

}
