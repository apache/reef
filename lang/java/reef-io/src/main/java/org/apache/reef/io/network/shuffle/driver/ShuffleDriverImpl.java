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
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
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
      final ShuffleDescription shuffleDescription, final Class<K> managerClass) {
    if (managerMap.containsKey(shuffleDescription.getShuffleName())) {
      throw new RuntimeException(shuffleDescription.getShuffleName() + " was already registered in ShuffleDriver");
    }
    final Injector forkedInjector = rootInjector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
    try {
      final K manager = forkedInjector.getInstance(managerClass);
      managerMap.put(shuffleDescription.getShuffleName(), manager);
      return manager;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K extends ShuffleManager> K getManager(String shuffleName) {
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
    System.out.println(taskId + " " + managerMap.values());
    for (final ShuffleManager manager : managerMap.values()) {
      final Configuration descriptionConf = manager.getShuffleDescriptionConfigurationForTask(taskId);

      if (descriptionConf != null) {
        final Configuration shuffleConf = Tang.Factory.getTang().newConfigurationBuilder(descriptionConf)
            .bindImplementation(ShuffleClient.class, manager.getClientClass())
            .build();
        System.out.println(confSerializer.toString(shuffleConf));
        confBuilder.bindSetEntry(ShuffleParameters.SerializedShuffleSet.class, confSerializer.toString(shuffleConf));
      }
    }
    return confBuilder.build();
  }
}
