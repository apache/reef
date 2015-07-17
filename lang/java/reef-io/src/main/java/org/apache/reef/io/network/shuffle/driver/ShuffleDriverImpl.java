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
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.network.*;
import org.apache.reef.io.network.shuffle.params.SerializedShuffleSet;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStartHandler;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStopHandler;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

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
  private final ConnectionFactory<ShuffleControlMessage> connectionFactory;
  private final IdentifierFactory idFactory;
  private final Identifier driverIdentifier;
  private final ConcurrentMap<String, ShuffleManager> managerMap;
  private final ConcurrentMap<String, ShuffleManagerMessageDispatcher> messageDispatcherMap;

  @Inject
  public ShuffleDriverImpl(
      final Injector injector,
      final ConfigurationSerializer confSerializer,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService,
      final ShuffleControlMessageCodec messageCodec,
      final ShuffleControlLinkListener linkListener,
      final ShuffleControlMessageHandler messageHandler) {
    this.injector = injector;
    this.confSerializer = confSerializer;
    this.linkListener = linkListener;
    this.messageHandler = messageHandler;
    this.managerMap = new ConcurrentHashMap<>();
    this.messageDispatcherMap = new ConcurrentHashMap<>();
    this.idFactory = idFactory;
    this.driverIdentifier = idFactory.getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_IDENTIFIER);
    final Identifier controlMessageIdentifier = idFactory.getNewInstance(ShuffleNetworkConnectionId.CONTROL_MESSAGE);
    try {
      networkConnectionService
          .registerConnectionFactory(controlMessageIdentifier, messageCodec, messageHandler, linkListener);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }

    this.connectionFactory = networkConnectionService.getConnectionFactory(controlMessageIdentifier);
  }

  @Override
  public <K extends ShuffleManager> K registerManager(
      final ShuffleDescription shuffleDescription, final Class<K> managerClass) {
    return registerManager(shuffleDescription, managerClass, null);
  }

  @Override
  public <K extends ShuffleManager> K registerManager(
      final ShuffleDescription shuffleDescription, Class<K> managerClass, final Configuration managerConf) {
    try {
      final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
      confBuilder.bindImplementation(ShuffleManager.class, managerClass);
      final Injector forkedInjector;

      if (managerConf == null) {
        forkedInjector = injector.forkInjector(confBuilder.build());
      } else {
        forkedInjector = injector.forkInjector(confBuilder.build(), managerConf);
      }

      forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
      forkedInjector.bindVolatileInstance(ShuffleDriver.class, this);
      final K manager = (K) forkedInjector.getInstance(ShuffleManager.class);
      if (managerMap.putIfAbsent(shuffleDescription.getShuffleName().getName(), manager) != null) {
        throw new RuntimeException(shuffleDescription.getShuffleName() + " was already submitted.");
      }

      final ShuffleManagerMessageDispatcher messageDispatcher = new ShuffleManagerMessageDispatcher(manager);

      linkListener.registerLinkListener(shuffleDescription.getShuffleName(), messageDispatcher.getControlLinkListener());
      messageHandler.registerMessageHandler(shuffleDescription.getShuffleName(), messageDispatcher.getControlMessageHandler());
      messageDispatcherMap.put(shuffleDescription.getShuffleName().getName(), messageDispatcher);

      for (final String groupingName : shuffleDescription.getGroupingNameList()) {
        final GroupingDescription groupingDescription = shuffleDescription.getGroupingDescription(groupingName);
        if (groupingDescription.getGroupingControllerClass() != null) {
          final Injector groupingControllerInjector = forkedInjector.forkInjector();
          groupingControllerInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);
          final GroupingController groupingController = (GroupingController) groupingControllerInjector.getInstance(groupingDescription.getGroupingControllerClass());
          registerGroupingController(shuffleDescription.getShuffleName().getName(), groupingController);
        }
      }

      return manager;
    } catch(final InjectionException exception) {
      throw new RuntimeException("An Injection error occurred while submitting topology "
          + shuffleDescription.getShuffleName(), exception);
    }
  }

  @Override
  public <K extends ShuffleManager> K getManager(final Class<? extends Name<String>> shuffleName) {
    return (K) managerMap.get(shuffleName.getName());
  }

  @Override
  public <K extends ShuffleManager> K getManager(String shuffleName) {
    return (K) managerMap.get(shuffleName);
  }

  @Override
  public void registerGroupingController(String shuffleName, GroupingController groupingController) {
    final String groupingName = groupingController.getGroupingDescription().getGroupingName();
    final ShuffleManagerMessageDispatcher dispatcher = messageDispatcherMap.get(shuffleName);
    dispatcher.registerControlMessageHandler(groupingName, groupingController);
    dispatcher.registerControlLinkListener(groupingName, groupingController);
  }


  @Override
  public void sendControlMessage(String destId, int code, String shuffleName, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    final ShuffleControlMessage controlMessage = new ShuffleControlMessage(code, shuffleName, groupingName, data, sourceType, sinkType);
    final Identifier destIdentifier = idFactory.getNewInstance(destId);
    try {
      final Connection<ShuffleControlMessage> connection = connectionFactory.newConnection(destIdentifier);
      connection.open();
      connection.write(new ShuffleControlMessage(code, shuffleName, groupingName, data, sourceType, sinkType));
    } catch (final NetworkException e) {
      messageDispatcherMap.get(shuffleName).getControlLinkListener()
          .onException(e, null, createNetworkShuffleControlMessage(destIdentifier, controlMessage));
    }
  }

  private Message<ShuffleControlMessage> createNetworkShuffleControlMessage(
      final Identifier destIdentifier, final ShuffleControlMessage controlMessage) {
    return new NSMessage<>(driverIdentifier, destIdentifier, controlMessage);
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
    for (final ShuffleManager manager : managerMap.values()) {
      final Configuration descriptionConf = manager.getShuffleDescriptionConfigurationForTask(taskId);

      if (descriptionConf != null) {
        final Configuration shuffleConf = Tang.Factory.getTang().newConfigurationBuilder(descriptionConf)
            .bindImplementation(ShuffleClient.class, manager.getClientClass())
            .build();
        confBuilder.bindSetEntry(SerializedShuffleSet.class, confSerializer.toString(shuffleConf));
      }
    }
    return confBuilder.build();
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
    for (final ShuffleManager manager : managerMap.values()) {
      manager.onRunningTask(runningTask);
    }
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
    for (final ShuffleManager manager : managerMap.values()) {
      manager.onFailedTask(failedTask);
    }
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
    for (final ShuffleManager manager : managerMap.values()) {
      manager.onCompletedTask(completedTask);
    }
  }
}
