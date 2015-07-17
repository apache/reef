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
package org.apache.reef.io.network.shuffle.network;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ShuffleTupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage>> {

  private final Map<String, Map<String, EventHandler>> eventHandlerMap;

  @Inject
  public ShuffleTupleMessageHandler() {
    eventHandlerMap = new ConcurrentHashMap<>();
  }

  public <K, V> void registerMessageHandler(String shuffleName, String groupingName,
                              EventHandler<Message<ShuffleTupleMessage<K, V>>> eventHandler) {
    if (!eventHandlerMap.containsKey(shuffleName)) {
      eventHandlerMap.put(shuffleName, new ConcurrentHashMap<String, EventHandler>());
    }

    eventHandlerMap.get(shuffleName).put(groupingName, eventHandler);
  }

  @Override
  public void onNext(final Message<ShuffleTupleMessage> message) {
    final ShuffleTupleMessage tupleMessage = message.getData().iterator().next();
    eventHandlerMap.get(tupleMessage.getShuffleGroupName()).get(tupleMessage.getShuffleName()).onNext(message);
  }
}