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
package org.apache.reef.bridge.driver.launch.azbatch;

import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the Java Driver configuration generator for .NET Drivers that generates
 * the Driver configuration at runtime. Called by {@link AzureBatchLauncher}.
 */
final class AzureBatchDriverConfigurationProvider {

  private static final Logger LOG = Logger.getLogger(AzureBatchDriverConfigurationProvider.class.getName());

  // The driver service configuration provider
  private final IDriverServiceConfigurationProvider driverServiceConfigurationProvider;

  // The driver runtime configuration provider
  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  private AzureBatchDriverConfigurationProvider(
      final DriverConfigurationProvider driverConfigurationProvider,
      final IDriverServiceConfigurationProvider driverServiceConfigurationProvider) {
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.driverServiceConfigurationProvider = driverServiceConfigurationProvider;
  }

  Configuration getDriverConfigurationFromParams(
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration) throws IOException {

    final String jobId = driverClientConfiguration.getJobid();
    final File jobFolder = new File(driverClientConfiguration.getDriverJobSubmissionDirectory());

    LOG.log(Level.INFO, "jobFolder {0} jobId {1}.", new Object[]{jobFolder.toURI(), jobId});

    return this.driverConfigurationProvider.getDriverConfiguration(
        jobFolder.toURI(), ClientRemoteIdentifier.NONE, jobId,
        driverServiceConfigurationProvider.getDriverServiceConfiguration(driverClientConfiguration));
  }
}
