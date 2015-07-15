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

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleCodec;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
final class TupleOperatorFactoryImpl implements TupleOperatorFactory {

  private final String nodeId;
  private final InjectionFuture<ShuffleClient> client;
  private final NetworkConnectionService networkConnectionService;
  private final Injector injector;

  private Map<String, TupleSender> senderMap;
  private Map<String, TupleReceiver> receiverMap;

  @Inject
  public TupleOperatorFactoryImpl(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String nodeId,
      final InjectionFuture<ShuffleClient> client,
      final NetworkConnectionService networkConnectionService,
      final Injector injector) {
    this.nodeId = nodeId;
    this.client = client;
    this.networkConnectionService = networkConnectionService;
    this.injector = injector;
    this.senderMap = new ConcurrentHashMap<>();
    this.receiverMap = new ConcurrentHashMap<>();
  }

  @Override
  public <K, V> TupleReceiver<K, V> newTupleReceiver(final GroupingDescription groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();

    if (!receiverMap.containsKey(groupingName)) {
      final ShuffleDescription description = client.get().getShuffleDescription();
      if (!description.getReceiverIdList(groupingName).contains(nodeId)) {
        throw new RuntimeException(groupingName + " does not have " + nodeId + " as a receiver.");
      }

      final Configuration receiverConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(GroupingStrategy.class, groupingDescription.getGroupingStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(receiverConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(NetworkConnectionService.class, networkConnectionService);
      forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);

      try {
        final TupleReceiver receiver = forkedInjector.getInstance(TupleReceiver.class);
        receiverMap.put(groupingName, receiver);
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting receiver with "
            + groupingDescription, e);
      }
    }

    return receiverMap.get(groupingName);
  }

  @Override
  public <K, V> TupleSender<K, V> newTupleSender(final GroupingDescription groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();

    if (!senderMap.containsKey(groupingName)) {
      final ShuffleDescription description = client.get().getShuffleDescription();
      if (!description.getSenderIdList(groupingName).contains(nodeId)) {
        throw new RuntimeException(groupingName + " does not have " + nodeId + " as a sender.");
      }

      final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(GroupingStrategy.class, groupingDescription.getGroupingStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(senderConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);
      forkedInjector.bindVolatileInstance(NetworkConnectionService.class, networkConnectionService);
      forkedInjector.bindVolatileParameter(ShuffleTupleCodec.class, client.get().getTupleCodec(groupingName));

      try {
        final TupleSender sender = forkedInjector.getInstance(TupleSender.class);
        senderMap.put(groupingName, sender);
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting sender with "
            + groupingDescription, e);
      }
    }

    return senderMap.get(groupingName);
  }
}
