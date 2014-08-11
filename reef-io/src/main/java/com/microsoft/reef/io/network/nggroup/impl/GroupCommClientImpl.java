/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedGroupConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 *
 */
public class GroupCommClientImpl implements com.microsoft.reef.io.network.nggroup.api.GroupCommClient {
  private static final Logger LOG = Logger.getLogger(GroupCommClientImpl.class.getName());

  private final Map<Class<? extends Name<String>>, CommunicationGroupClient> communicationGroups;

  @Inject
  public GroupCommClientImpl(
      @Parameter(SerializedGroupConfigs.class) final Set<String> groupConfigs,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
      final GroupCommNetworkHandler groupCommNetworkHandler,
      final NetworkService<GroupCommMessage> netService,
      final ConfigurationSerializer configSerializer
  ) {
    this.communicationGroups = new HashMap<>();
    LOG.info("GroupCommHandler-" + groupCommNetworkHandler.toString());
    for (final String groupConfigStr : groupConfigs) {
      try {
        final Configuration groupConfig = configSerializer.fromString(groupConfigStr);

        final Injector injector = Tang.Factory.getTang().newInjector(groupConfig);
        injector.bindVolatileParameter(TaskConfigurationOptions.Identifier.class, taskId);
        injector.bindVolatileInstance(GroupCommNetworkHandler.class, groupCommNetworkHandler);
        injector.bindVolatileInstance(NetworkService.class, netService);

        final CommunicationGroupClient commGroupClient = injector.getInstance(CommunicationGroupClient.class);

        this.communicationGroups.put(commGroupClient.getName(), commGroupClient);

      } catch (BindException | IOException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      } catch (final InjectionException e) {
        throw new RuntimeException("Unable to deserialize operator config", e);
      }
    }

  }

  @Override
  public CommunicationGroupClient getCommunicationGroup(
      final Class<? extends Name<String>> groupName) {
    return communicationGroups.get(groupName);
  }

  @Override
  public void waitForDriver(final long timeout, final TimeUnit unit)
      throws TimeoutException {
    // TODO Auto-generated method stub

  }
}
