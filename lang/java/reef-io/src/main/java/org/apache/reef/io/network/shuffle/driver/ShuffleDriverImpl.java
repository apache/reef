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

import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStartHandler;
import org.apache.reef.io.network.shuffle.task.ShuffleContextStopHandler;
import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
final class ShuffleDriverImpl implements ShuffleDriver {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;
  private final ConcurrentMap<String, ShuffleManager> managerMap;

  @Inject
  public ShuffleDriverImpl(
      final Injector rootInjector,
      final ConfigurationSerializer confSerializer) {
    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.managerMap = new ConcurrentHashMap<>();
  }

  @Override
  public <K extends ShuffleManager> K registerManager(
      final ShuffleGroupDescription shuffleGroupDescription, final Class<K> managerClass) {
    if (managerMap.containsKey(shuffleGroupDescription.getShuffleGroupName())) {
      throw new RuntimeException(shuffleGroupDescription.getShuffleGroupName()
          + " was already registered in ShuffleDriver");
    }
    final Injector forkedInjector = rootInjector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleGroupDescription.class, shuffleGroupDescription);
    try {
      final K manager = forkedInjector.getInstance(managerClass);
      managerMap.put(shuffleGroupDescription.getShuffleGroupName(), manager);
      return manager;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K extends ShuffleManager> K getManager(final String shuffleName) {
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
      final Configuration clientConf = manager.getClientConfigurationForTask(taskId);

      if (clientConf != null) {
        final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder(clientConf)
            .bindImplementation(ShuffleClient.class, manager.getClientClass())
            .build();
        confBuilder.bindSetEntry(ShuffleParameters.SerializedClientSet.class, confSerializer.toString(conf));
      }
    }
    return confBuilder.build();
  }
}
