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
package org.apache.reef.io.network.shuffle.driver;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlLinkListener;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessageCodec;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessageHandler;
import org.apache.reef.io.network.shuffle.params.SerializedTopologySet;
import org.apache.reef.io.network.shuffle.params.ShuffleControlMessageNSId;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStartHandler;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStopHandler;
import org.apache.reef.io.network.shuffle.topology.TopologyDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
final class ShuffleDriverImpl implements ShuffleDriver {

  private final Injector injector;
  private final ConfigurationSerializer confSerializer;
  private final ShuffleControlLinkListener linkListener;
  private final ShuffleControlMessageHandler messageHandler;
  private final ConcurrentMap<Class<? extends Name>, ShuffleTopologyManager> topologyManagerMap;

  @Inject
  public ShuffleDriverImpl(
      final Injector injector,
      final ConfigurationSerializer confSerializer,
      final NetworkServiceClient nsClient,
      final ShuffleControlMessageCodec messageCodec,
      final ShuffleControlLinkListener linkListener,
      final ShuffleControlMessageHandler messageHandler) {
    this.injector = injector;
    this.confSerializer = confSerializer;
    this.linkListener = linkListener;
    this.messageHandler = messageHandler;
    this.topologyManagerMap = new ConcurrentHashMap<>();

    try {
      nsClient.registerConnectionFactory(ShuffleControlMessageNSId.class, messageCodec, messageHandler, linkListener);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ShuffleTopologyManager submitTopology(final TopologyDescription topologyDescription) {
    try {
      final Injector forkedInjector;

      if (topologyDescription.getManagerConfiguration() == null) {
        forkedInjector = injector.forkInjector();
      } else {
        forkedInjector = injector.forkInjector(topologyDescription.getManagerConfiguration());
      }

      forkedInjector.bindVolatileInstance(TopologyDescription.class, topologyDescription);
      final ShuffleTopologyManager manager = forkedInjector.getInstance(topologyDescription.getManagerClass());
      if (topologyManagerMap.putIfAbsent(topologyDescription.getTopologyName(), manager) != null) {
        throw new RuntimeException(topologyDescription.getTopologyName() + " was already submitted.");
      }

      linkListener.registerLinkListener(manager.getTopologyName(), manager.getControlLinkListener());
      messageHandler.registerMessageHandler(manager.getTopologyName(), manager.getControlMessageHandler());
      return manager;
    } catch(final InjectionException exception) {
      throw new RuntimeException("An Injection error occurred while submitting topology "
          + topologyDescription.getTopologyName(), exception);
    }
  }

  @Override
  public ShuffleTopologyManager getTopologyManager(final Class<? extends Name<String>> topologyName) {
    return topologyManagerMap.get(topologyName);
  }

  @Override
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, ShuffleContextStartHandler.class)
        .bindSetEntry(ContextStopHandlers.class, ShuffleContextStopHandler.class)
        .build();
  }

  @Override
  public Configuration getTaskConfiguration(final String taskId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (ShuffleTopologyManager manager : topologyManagerMap.values()) {
      final Configuration topologyConf = manager.getTopologyConfigurationForTask(taskId);
      if (topologyConf != null) {
        confBuilder.bindSetEntry(SerializedTopologySet.class, confSerializer.toString(topologyConf));
      }
    }
    return confBuilder.build();
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
    for (final ShuffleTopologyManager manager : topologyManagerMap.values()) {
      manager.onRunningTask(runningTask);
    }
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
    for (final ShuffleTopologyManager manager : topologyManagerMap.values()) {
      manager.onFailedTask(failedTask);
    }
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
    for (final ShuffleTopologyManager manager : topologyManagerMap.values()) {
      manager.onCompletedTask(completedTask);
    }
  }
}
