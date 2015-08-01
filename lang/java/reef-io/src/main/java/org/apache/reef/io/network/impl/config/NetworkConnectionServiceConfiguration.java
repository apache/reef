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

package org.apache.reef.io.network.impl.config;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverStartHandler;
import org.apache.reef.io.network.impl.BindNetworkConnectionServiceToDriver;
import org.apache.reef.io.network.impl.BindNetworkConnectionServiceToTask;
import org.apache.reef.io.network.impl.NetworkConnectionServiceImpl;
import org.apache.reef.io.network.impl.UnbindNetworkConnectionServiceFromTask;
import org.apache.reef.io.network.naming.LocalNameResolverImpl;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

/**
 * Configuration for NetworkConnectionService.
 */
public final class NetworkConnectionServiceConfiguration {

  private NetworkConnectionServiceConfiguration() {
    //not called
  }

  /**
   * Get a network connection configuration for driver.
   * @return network service configuration
   */
  public static Configuration getDriverConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(NameResolver.class, LocalNameResolverImpl.class);
    jcb.bindSetEntry(DriverStartHandler.class, BindNetworkConnectionServiceToDriver.class);
    return jcb.build();
  }

  /**
   * Get a network service configuration for registering network service to a task.
   * @param nameServerAddr name server address
   * @param nameServerPort namer server port
   * @return network service configuration
   */
  public static Configuration getServiceConfiguration(final String nameServerAddr, final int nameServerPort) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NameResolverNameServerAddr.class, nameServerAddr);
    jcb.bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServerPort));
    return Configurations.merge(jcb.build(), ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, NetworkConnectionServiceImpl.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, BindNetworkConnectionServiceToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP, UnbindNetworkConnectionServiceFromTask.class)
        .build());
  }
}