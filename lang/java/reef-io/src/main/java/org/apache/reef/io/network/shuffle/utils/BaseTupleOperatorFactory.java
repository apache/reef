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
package org.apache.reef.io.network.shuffle.utils;

import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.params.ReceiverIdList;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleCodec;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.topology.GroupingDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 *
 */
public final class BaseTupleOperatorFactory {

  private final String nodeId;
  private final ShuffleClient client;
  private final ConnectionFactory<ShuffleTupleMessage> connFactory;
  private final Injector injector;

  public BaseTupleOperatorFactory(
      final String nodeId,
      final ShuffleClient client,
      final ConnectionFactory<ShuffleTupleMessage> connFactory,
      final Injector injector) {
    this.nodeId = nodeId;
    this.client = client;
    this.connFactory = connFactory;
    this.injector = injector;
  }

  public BaseTupleReceiver createReceiverWith(final GroupingDescriptor groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();
    if (!client.getReceiverIdList(groupingName).contains(nodeId)) {
      throw new RuntimeException(groupingName + " does not have " + nodeId + " as a receiver.");
    }

    final Injector forkedInjector = injector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleClient.class, client);
    forkedInjector.bindVolatileInstance(GroupingDescriptor.class, groupingDescription);

    try {
      return forkedInjector.getInstance(BaseTupleReceiver.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing receiver with " + groupingDescription, e);
    }
  }

  public BaseTupleSender createSenderWith(final GroupingDescriptor groupingDescription) {
    final String groupingName = groupingDescription.getGroupingName();
    if (!client.getSenderIdList(groupingName).contains(nodeId)) {
      throw new RuntimeException(groupingName + " does not have " + nodeId + " as a sender.");
    }

    final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(GroupingStrategy.class, groupingDescription.getGroupingStrategyClass())
        .build();

    final Injector forkedInjector = injector.forkInjector(senderConfiguration);
    forkedInjector.bindVolatileParameter(ReceiverIdList.class, client.getReceiverIdList(groupingName));
    forkedInjector.bindVolatileInstance(ShuffleClient.class, client);
    forkedInjector.bindVolatileInstance(GroupingDescriptor.class, groupingDescription);
    forkedInjector.bindVolatileInstance(ConnectionFactory.class, connFactory);
    forkedInjector.bindVolatileParameter(ShuffleTupleCodec.class, client.getTupleCodec(groupingName));

    try {
      return forkedInjector.getInstance(BaseTupleSender.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
    }
  }
}
