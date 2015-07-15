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
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlLinkListener;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessageCodec;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessageHandler;
import org.apache.reef.io.network.shuffle.ns.ShuffleNetworkConnectionId;
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
  private final ConcurrentMap<Class<? extends Name>, ShuffleManager> managerMap;

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

    try {
      networkConnectionService
          .registerConnectionFactory(idFactory.getNewInstance(ShuffleNetworkConnectionId.CONTROL_MESSAGE)
              , messageCodec, messageHandler, linkListener);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }
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
      final Injector forkedInjector;

      if (managerConf == null) {
        forkedInjector = injector.forkInjector();
      } else {
        forkedInjector = injector.forkInjector(managerConf);
      }

      forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
      final K manager = forkedInjector.getInstance(managerClass);
      if (managerMap.putIfAbsent(shuffleDescription.getShuffleName(), manager) != null) {
        throw new RuntimeException(shuffleDescription.getShuffleName() + " was already submitted.");
      }

      linkListener.registerLinkListener(shuffleDescription.getShuffleName(), manager.getControlLinkListener());
      messageHandler.registerMessageHandler(shuffleDescription.getShuffleName(), manager.getControlMessageHandler());
      return manager;
    } catch(final InjectionException exception) {
      throw new RuntimeException("An Injection error occurred while submitting topology "
          + shuffleDescription.getShuffleName(), exception);
    }
  }

  @Override
  public <K extends ShuffleManager> K getManager(final Class<? extends Name<String>> shuffleName) {
    return (K) managerMap.get(shuffleName);
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
