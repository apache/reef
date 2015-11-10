/*
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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.io.network.group.api.task.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.config.parameters.SerializedGroupConfigs;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GroupCommClientImpl implements GroupCommClient {
  private static final Logger LOG = Logger.getLogger(GroupCommClientImpl.class.getName());

  private final Map<Class<? extends Name<String>>, CommunicationGroupServiceClient> communicationGroups =
      new HashMap<>();

  /**
   * @deprecated in 0.14.
   * Use the other constructor that receives an {@code injector} as a parameter instead.
   * The parameters {@code taskId} and {@code netService} can be removed from the other constructor when
   * this constructor gets deleted.
   */
  @Deprecated
  @Inject
  public GroupCommClientImpl(
      @Parameter(SerializedGroupConfigs.class) final Set<String> groupConfigs,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
      final GroupCommNetworkHandler groupCommNetworkHandler,
      final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService,
      final ConfigurationSerializer configSerializer) {

    LOG.log(Level.FINEST, "GroupCommHandler-{0}", groupCommNetworkHandler);

    for (final String groupConfigStr : groupConfigs) {
      try {
        final Configuration groupConfig = configSerializer.fromString(groupConfigStr);

        final Injector injector = Tang.Factory.getTang().newInjector(groupConfig);
        injector.bindVolatileParameter(TaskConfigurationOptions.Identifier.class, taskId);
        injector.bindVolatileInstance(GroupCommNetworkHandler.class, groupCommNetworkHandler);
        injector.bindVolatileInstance(NetworkService.class, netService);

        final CommunicationGroupServiceClient commGroupClient =
            injector.getInstance(CommunicationGroupServiceClient.class);

        this.communicationGroups.put(commGroupClient.getName(), commGroupClient);

      } catch (final InjectionException | IOException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      }
    }
  }

  @Inject
  private GroupCommClientImpl(@Parameter(SerializedGroupConfigs.class) final Set<String> groupConfigs,
                              @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                              final GroupCommNetworkHandler groupCommNetworkHandler,
                              final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> netService,
                              final ConfigurationSerializer configSerializer,
                              final Injector injector) {

    LOG.log(Level.FINEST, "GroupCommHandler-{0}", groupCommNetworkHandler);

    for (final String groupConfigStr : groupConfigs) {
      try {
        final Configuration groupConfig = configSerializer.fromString(groupConfigStr);
        final Injector forkedInjector = injector.forkInjector(groupConfig);

        final CommunicationGroupServiceClient commGroupClient =
            forkedInjector.getInstance(CommunicationGroupServiceClient.class);

        this.communicationGroups.put(commGroupClient.getName(), commGroupClient);

      } catch (final InjectionException | IOException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      }
    }
  }

  @Override
  public CommunicationGroupClient getCommunicationGroup(
      final Class<? extends Name<String>> groupName) {
    return communicationGroups.get(groupName);
  }
}
