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
import org.apache.reef.io.network.shuffle.params.SerializedShuffleSet;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
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

  private final Map<String, ShuffleClient> clientMap;
  private final GlobalTupleCodecMap globalTupleCodecMap;

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;

  private final ShuffleControlMessageHandler shuffleControlMessageHandler;
  private final ShuffleControlLinkListener shuffleControlLinkListener;

  private final ShuffleTupleMessageHandler shuffleTupleMessageHandler;
  private final ShuffleTupleLinkListener shuffleTupleLinkListener;

  @Inject
  public ShuffleServiceImpl(
      final Injector rootInjector,
      final @Parameter(SerializedShuffleSet.class) Set<String> serializedTopologySet,
      final ConfigurationSerializer confSerializer,
      final ShuffleControlMessageHandler shuffleControlMessageHandler,
      final ShuffleControlLinkListener shuffleControlLinkListener,
      final ShuffleTupleMessageHandler shuffleTupleMessageHandler,
      final ShuffleTupleLinkListener shuffleTupleLinkListener,
      final GlobalTupleCodecMap globalTupleCodecMap) {

    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.clientMap = new HashMap<>();
    this.globalTupleCodecMap = globalTupleCodecMap;
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
      final ShuffleClient client = injector.getInstance(ShuffleClient.class);
      final ShuffleDescription description = client.getShuffleDescription();
      shuffleControlLinkListener.registerLinkListener(description.getShuffleName(), client.getControlLinkListener());
      shuffleControlMessageHandler.registerMessageHandler(description.getShuffleName(), client.getControlMessageHandler());
      shuffleTupleLinkListener.registerLinkListener(description.getShuffleName(), client.getTupleLinkListener());
      shuffleTupleMessageHandler.registerMessageHandler(description.getShuffleName(), client.getTupleMessageHandler());
      clientMap.put(description.getShuffleName().getName(), client);
      registTupleCodecs(client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing topology " + serializedTopology, exception);
    }
  }

  private void registTupleCodecs(final ShuffleClient client) {
    final ShuffleDescription description = client.getShuffleDescription();
    for (final String groupingName : description.getGroupingNameList()) {
      globalTupleCodecMap.registerTupleCodec(description.getShuffleName().getName(), groupingName, client.getTupleCodec(groupingName));
    }
  }

  @Override
  public ShuffleClient getClient(final Class<? extends Name<String>> clientName) {
    return clientMap.get(clientName.getName());
  }

  @Override
  public ShuffleClient getClient(final String topologyName) {
    return clientMap.get(topologyName);
  }
}
