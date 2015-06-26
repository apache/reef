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
package org.apache.reef.io.network.impl;


import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.io.network.naming.NameClient;
import org.apache.reef.io.network.naming.NameClientLocalImpl;
import org.apache.reef.io.network.naming.NameClientRemoteImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Configuration Module Builder for EvaluatorSide NetworkService.
 */
public final class NetworkServiceConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<NameClient> NAME_CLIENT = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new NetworkServiceConfiguration()
      .bindNamedParameter(NetworkServiceParameters.NameClientImpl.class, NAME_CLIENT)
      .build();

  /**
   * NetworkServiceConfiguration for Evalautor
   */
  public static Configuration getServiceConfiguration() {
    Configuration conf1 = ServiceConfiguration.CONF
                .set(ServiceConfiguration.SERVICES, DefaultNetworkServiceImpl.class)
                .set(ServiceConfiguration.ON_TASK_STARTED, BindNSToTask.class)
                .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSFromTask.class)
                .build();

    return Configurations.merge(conf1, CONF.set(NAME_CLIENT, NameClientRemoteImpl.class).build());
  }

  /**
   * NetworkServiceConfiguartion for Driver
   */
  public static Configuration getDriverServiceConfiguration() {
    return CONF.set(NAME_CLIENT, NameClientLocalImpl.class).build();
  }
}
