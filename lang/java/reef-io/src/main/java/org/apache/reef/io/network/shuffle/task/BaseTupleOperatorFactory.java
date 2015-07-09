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

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleCodec;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleMessageNSId;
import org.apache.reef.io.network.shuffle.descriptor.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class BaseTupleOperatorFactory implements TupleOperatorFactory {

  private final String nodeId;
  private final InjectionFuture<ShuffleClient> client;
  private final ConnectionFactory<ShuffleTupleMessage> connFactory;
  private final Injector injector;

  private Map<String, BaseTupleSender> senderMap;
  private Map<String, BaseTupleReceiver> receiverMap;

  @Inject
  public BaseTupleOperatorFactory(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String nodeId,
      final InjectionFuture<ShuffleClient> client,
      final NetworkServiceClient nsClient,
      final Injector injector) {
    this.nodeId = nodeId;
    this.client = client;
    this.connFactory = nsClient.getConnectionFactory(ShuffleTupleMessageNSId.class);
    this.injector = injector;
    this.senderMap = new HashMap<>();
    this.receiverMap = new HashMap<>();
  }

  @Override
  public <K, V> BaseTupleReceiver<K, V> newTupleReceiver(final GroupingDescriptor groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();

    if (!receiverMap.containsKey(groupingName)) {
      final ShuffleDescriptor descriptor = client.get().getShuffleDescriptor();
      if (!descriptor.getReceiverIdList(groupingName).contains(nodeId)) {
        throw new RuntimeException(groupingName + " does not have " + nodeId + " as a receiver.");
      }

      final Configuration receiverConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(GroupingStrategy.class, groupingDescription.getGroupingStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(receiverConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(GroupingDescriptor.class, groupingDescription);

      try {
        receiverMap.put(groupingName, forkedInjector.getInstance(BaseTupleReceiver.class));
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while deserializing receiver with " + groupingDescription, e);
      }
    }

    return receiverMap.get(groupingName);
  }

  @Override
  public <K, V> BaseTupleSender<K, V> newTupleSender(final GroupingDescriptor groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();

    if (!senderMap.containsKey(groupingName)) {
      final ShuffleDescriptor descriptor = client.get().getShuffleDescriptor();
      if (!descriptor.getSenderIdList(groupingName).contains(nodeId)) {
        throw new RuntimeException(groupingName + " does not have " + nodeId + " as a sender.");
      }

      final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(GroupingStrategy.class, groupingDescription.getGroupingStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(senderConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(GroupingDescriptor.class, groupingDescription);
      forkedInjector.bindVolatileInstance(ConnectionFactory.class, connFactory);
      forkedInjector.bindVolatileParameter(ShuffleTupleCodec.class, client.get().getTupleCodec(groupingName));

      try {
        senderMap.put(groupingName, forkedInjector.getInstance(BaseTupleSender.class));
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
      }
    }

    return senderMap.get(groupingName);
  }
}
