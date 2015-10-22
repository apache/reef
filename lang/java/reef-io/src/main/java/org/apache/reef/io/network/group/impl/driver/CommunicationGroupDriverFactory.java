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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

/**
 * A factory used to create CommunicationGroupDriver instance.
 * Uses Tang to instantiate new object.
 */
public final class CommunicationGroupDriverFactory {

  private final Injector injector;

  @Inject
  private CommunicationGroupDriverFactory(final Injector injector) {
    this.injector = injector;
  }

  /**
   * Instantiates a new CommunicationGroupDriver instance.
   * @param groupName specified name of the communication group
   * @param numberOfTasks minimum number of tasks needed in this group before start
   * @param customFanOut fanOut for TreeTopology
   * @return CommunicationGroupDriver instance
   * @throws InjectionException
   */
  public CommunicationGroupDriver getNewInstance(
      final Class<? extends Name<String>> groupName,
      final BroadcastingEventHandler<GroupCommunicationMessage> commGroupMessageHandler,
      final int numberOfTasks,
      final int customFanOut) throws InjectionException {

    final Injector newInjector = injector.forkInjector();
    newInjector.bindVolatileParameter(CommGroupNameClass.class, groupName);
    newInjector.bindVolatileParameter(CommGroupMessageHandler.class, commGroupMessageHandler);
    newInjector.bindVolatileParameter(CommGroupNumTask.class, numberOfTasks);
    newInjector.bindVolatileParameter(TreeTopologyFanOut.class, customFanOut);
    return newInjector.getInstance(CommunicationGroupDriver.class);
  }
}
