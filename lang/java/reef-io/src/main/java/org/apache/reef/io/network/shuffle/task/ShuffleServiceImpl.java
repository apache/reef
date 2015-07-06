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

import org.apache.reef.io.network.shuffle.ns.*;
import org.apache.reef.io.network.shuffle.params.SerializedTopologySet;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class ShuffleServiceImpl implements ShuffleService {

  private final Map<String, ShuffleTopologyClient> clientMap;
  private final TupleCodecMap tupleCodecMap;

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;

  private final ShuffleControlMessageHandler shuffleControlMessageHandler;
  private final ShuffleControlLinkListener shuffleControlLinkListener;

  private final ShuffleTupleMessageHandler shuffleTupleMessageHandler;
  private final ShuffleTupleLinkListener shuffleTupleLinkListener;

  @Inject
  public ShuffleServiceImpl(
      final Injector rootInjector,
      final @Parameter(SerializedTopologySet.class) Set<String> serializedTopologySet,
      final TupleCodecMap tupleCodecMap,
      final ConfigurationSerializer confSerializer,
      final ShuffleControlMessageHandler shuffleControlMessageHandler,
      final ShuffleControlLinkListener shuffleControlLinkListener,
      final ShuffleTupleMessageHandler shuffleTupleMessageHandler,
      final ShuffleTupleLinkListener shuffleTupleLinkListener) {
    this.rootInjector = rootInjector;
    this.tupleCodecMap = tupleCodecMap;
    this.confSerializer = confSerializer;
    this.clientMap = new HashMap<>();
    this.shuffleControlLinkListener = shuffleControlLinkListener;
    this.shuffleControlMessageHandler = shuffleControlMessageHandler;
    this.shuffleTupleMessageHandler = shuffleTupleMessageHandler;
    this.shuffleTupleLinkListener = shuffleTupleLinkListener;
    deserializeClients(serializedTopologySet);
  }

  private void deserializeClients(final Set<String> serializedTopologySet) {
    for (final String serializedTopology : serializedTopologySet) {
      deserializeClient(serializedTopology);
    }
  }

  private void deserializeClient(final String serializedTopology) {
    try {
      final Configuration topologyConfig = confSerializer.fromString(serializedTopology);
      final Injector injector = rootInjector.forkInjector(topologyConfig);
      final ShuffleTopologyClient client = injector.getInstance(ShuffleTopologyClient.class);
      shuffleControlLinkListener.registerLinkListener(client.getTopologyName(), client.getControlLinkListener());
      shuffleControlMessageHandler.registerMessageHandler(client.getTopologyName(), client.getControlMessageHandler());
      shuffleTupleLinkListener.registerLinkListener(client.getTopologyName(), client.getTupleLinkListener());
      shuffleTupleMessageHandler.registerMessageHandler(client.getTopologyName(), client.getTupleMessageHandler());
      clientMap.put(client.getTopologyName().getName(), client);
      tupleCodecMap.registerTupleCodecs(client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing topology " + serializedTopology, exception);
    }
  }

  @Override
  public ShuffleTopologyClient getTopologyClient(final Class<? extends Name<String>> topologyName) {
    return clientMap.get(topologyName.getName());
  }
}
