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
package org.apache.reef.runtime.azbatch.evaluator;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.ContainerIdentifier;
import org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.ContainerBasedLocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.SetTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortSet;

/**
 * ConfigurationModule to create evaluator shim configurations.
 */
@Private
@EvaluatorSide
public final class EvaluatorShimConfiguration extends ConfigurationModuleBuilder {

  /**
   * @see org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier
   */
  public static final RequiredParameter<String> DRIVER_REMOTE_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier
   */
  public static final RequiredParameter<String> CONTAINER_IDENTIFIER = new RequiredParameter<>();

  /**
   * Set of TCP Ports.
   */
  public static final OptionalParameter<Integer> TCP_PORT_SET = new OptionalParameter<>();

  private static final ConfigurationModule CONF = new EvaluatorShimConfiguration()
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindNamedParameter(DriverRemoteIdentifier.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(ContainerIdentifier.class, CONTAINER_IDENTIFIER)
      .bindSetEntry(TcpPortSet.class, TCP_PORT_SET)
      .build();

  public static final ConfigurationModule getConfigurationModule(boolean includeContainerConfiguration) {
    ConfigurationModuleBuilder shimConfigurationBuilder = EvaluatorShimConfiguration.CONF.getBuilder();

    // If using docker containers, then use a different set of bindings
    if (includeContainerConfiguration) {
      shimConfigurationBuilder = shimConfigurationBuilder
          .bindImplementation(LocalAddressProvider.class, ContainerBasedLocalAddressProvider.class)
          .bindImplementation(TcpPortProvider.class, SetTcpPortProvider.class);
    }

    return shimConfigurationBuilder.build();
  }
}
