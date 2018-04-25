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

package org.apache.reef.bridge.driver.service.grpc;

import org.apache.reef.bridge.driver.service.DriverServiceConfiguration;
import org.apache.reef.bridge.driver.service.DriverServiceConfigurationProviderBase;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * GRPC driver service configuration provider.
 */
public final class GRPCDriverServiceConfigurationProvider extends DriverServiceConfigurationProviderBase {

  private static final Logger LOG = Logger.getLogger(GRPCDriverServiceConfigurationProvider.class.getName());

  @Inject
  private GRPCDriverServiceConfigurationProvider() {
  }

  @Override
  public Configuration getConfiguration(final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    Configuration driverServiceConfiguration = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.DRIVER_SERVICE_IMPL, GRPCDriverService.class)
        .set(DriverServiceConfiguration.DRIVER_CLIENT_COMMAND, driverConfiguration.getDriverClientLaunchCommand())
        .build();
    return Configurations.merge(
        driverServiceConfiguration,
        getDriverConfiguration(driverConfiguration),
        getTcpPortRangeConfiguration(driverConfiguration));
  }
}
