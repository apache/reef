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

import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class ShuffleServiceImpl implements ShuffleService {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;

  private final Map<String, ShuffleClient> clientMap;

  @Inject
  public ShuffleServiceImpl(
      final Injector rootInjector,
      @Parameter(ShuffleParameters.SerializedShuffleSet.class) final Set<String> serializedTopologySet,
      final ConfigurationSerializer confSerializer) {

    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.clientMap = new ConcurrentHashMap<>();
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
      clientMap.put(description.getShuffleName(), client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing topology " + serializedTopology, exception);
    }
  }

  @Override
  public ShuffleClient getClient(final String topologyName) {
    return clientMap.get(topologyName);
  }
}
