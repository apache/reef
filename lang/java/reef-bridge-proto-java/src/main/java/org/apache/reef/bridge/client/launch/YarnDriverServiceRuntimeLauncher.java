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

package org.apache.reef.bridge.client.launch;

import org.apache.reef.bridge.client.IDriverRuntimeConfigurationProvider;
import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.client.IDriverServiceRuntimeLauncher;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Yarn driver service launcher.
 */
public final class YarnDriverServiceRuntimeLauncher implements IDriverServiceRuntimeLauncher {

  private static final Logger LOG = Logger.getLogger(YarnDriverServiceRuntimeLauncher.class.getName());

  private final IDriverRuntimeConfigurationProvider driverRuntimeConfigurationProvider;

  private final IDriverServiceConfigurationProvider driverServiceConfigurationProvider;

  @Inject
  private YarnDriverServiceRuntimeLauncher(
      final IDriverRuntimeConfigurationProvider driverRuntimeConfigurationProvider,
      final IDriverServiceConfigurationProvider driverServiceConfigurationProvider) {
    this.driverRuntimeConfigurationProvider = driverRuntimeConfigurationProvider;
    this.driverServiceConfigurationProvider = driverServiceConfigurationProvider;
  }

  @Override
  public void launch(final ClientProtocol.DriverClientConfiguration driverClientConfiguration) {
    try {
      final LauncherStatus status = DriverLauncher.getLauncher(
          driverRuntimeConfigurationProvider.getConfiguration(driverClientConfiguration))
          .run(driverServiceConfigurationProvider.getDriverServiceConfiguration(driverClientConfiguration));
      LOG.log(Level.INFO, "Job complete status: " + status.toString());
      if (status.getError().isPresent()) {
        LOG.log(Level.SEVERE, status.getError().get().getMessage());
        status.getError().get().printStackTrace();
      }
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
