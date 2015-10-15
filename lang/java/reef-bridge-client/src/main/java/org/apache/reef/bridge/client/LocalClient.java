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
package org.apache.reef.bridge.client;

import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.client.DriverConfigurationProvider;
import org.apache.reef.runtime.local.client.PreparedDriverFolderLauncher;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Submits a folder containing a Driver to the local runtime.
 */
public final class LocalClient {

  private static final Logger LOG = Logger.getLogger(LocalClient.class.getName());
  private static final String CLIENT_REMOTE_ID = ClientRemoteIdentifier.NONE;
  private final AvroConfigurationSerializer configurationSerializer;
  private final PreparedDriverFolderLauncher launcher;
  private final REEFFileNames fileNames;
  private final DriverConfigurationProvider driverConfigurationProvider;
  private final Set<ConfigurationProvider> configurationProviders;

  @Inject
  private LocalClient(final AvroConfigurationSerializer configurationSerializer,
                      final PreparedDriverFolderLauncher launcher,
                      final REEFFileNames fileNames,
                      final DriverConfigurationProvider driverConfigurationProvider,
                      @Parameter(DriverConfigurationProviders.class)
                      final Set<ConfigurationProvider> configurationProviders) {
    this.configurationSerializer = configurationSerializer;
    this.launcher = launcher;
    this.fileNames = fileNames;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.configurationProviders = configurationProviders;
  }

  private void submit(final LocalSubmissionFromCS localSubmissionFromCS) throws IOException {
    final File driverFolder = new File(localSubmissionFromCS.getJobFolder(),
        PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);
    if (!driverFolder.exists()) {
      throw new IOException("The Driver folder " + driverFolder.getAbsolutePath() + " doesn't exist.");
    }

    final Configuration driverConfiguration1 = driverConfigurationProvider
        .getDriverConfiguration(localSubmissionFromCS.getJobFolder(), CLIENT_REMOTE_ID,
            localSubmissionFromCS.getJobId(), Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER);
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final ConfigurationProvider configurationProvider : this.configurationProviders) {
      configurationBuilder.addConfiguration(configurationProvider.getConfiguration());
    }
    final Configuration providedConfigurations = configurationBuilder.build();
    final Configuration driverConfiguration = Configurations.merge(
        driverConfiguration1,
        Tang.Factory.getTang()
            .newConfigurationBuilder()
            .bindNamedParameter(JobSubmissionDirectory.class, driverFolder.toString())
            .build(),
        providedConfigurations);
    final File driverConfigurationFile = new File(driverFolder, fileNames.getDriverConfigurationPath());
    configurationSerializer.toFile(driverConfiguration, driverConfigurationFile);
    launcher.launch(driverFolder, localSubmissionFromCS.getJobId(), CLIENT_REMOTE_ID);
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final LocalSubmissionFromCS localSubmissionFromCS = LocalSubmissionFromCS.fromCommandLine(args);
    LOG.log(Level.INFO, "Local job submission received from C#: {0}", localSubmissionFromCS);
    final Configuration runtimeConfiguration = localSubmissionFromCS.getRuntimeConfiguration();

    final LocalClient client = Tang.Factory.getTang()
        .newInjector(runtimeConfiguration)
        .getInstance(LocalClient.class);

    client.submit(localSubmissionFromCS);
  }
}