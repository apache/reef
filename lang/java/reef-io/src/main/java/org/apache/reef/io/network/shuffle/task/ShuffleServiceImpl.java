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

import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescription;
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
      @Parameter(ShuffleParameters.SerializedClientSet.class) final Set<String> serializedClientSet,
      final ConfigurationSerializer confSerializer) {

    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.clientMap = new ConcurrentHashMap<>();
    deserializeClientSet(serializedClientSet);
  }

  private void deserializeClientSet(final Set<String> serializedClientSet) {
    for (final String serializedClient : serializedClientSet) {
      deserializeClient(serializedClient);
    }
  }

  private void deserializeClient(final String serializedClient) {
    try {
      final Configuration clientConfig = confSerializer.fromString(serializedClient);
      final Injector injector = rootInjector.forkInjector(clientConfig);
      final ShuffleClient client = injector.getInstance(ShuffleClient.class);
      final ShuffleGroupDescription description = client.getShuffleGroupDescription();
      clientMap.put(description.getShuffleGroupName(), client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing client "
          + serializedClient, exception);
    }
  }

  @Override
  public ShuffleClient getClient(final String shuffleGroupName) {
    return clientMap.get(shuffleGroupName);
  }
}
