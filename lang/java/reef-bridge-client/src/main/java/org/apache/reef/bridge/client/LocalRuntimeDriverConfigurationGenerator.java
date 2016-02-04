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
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.client.DriverConfigurationProvider;
import org.apache.reef.runtime.local.client.PreparedDriverFolderLauncher;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
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
 * Does client side manipulation of driver configuration for local runtime.
 */
final class LocalRuntimeDriverConfigurationGenerator {
  private static final Logger LOG = Logger.getLogger(LocalRuntimeDriverConfigurationGenerator.class.getName());
  private final REEFFileNames fileNames;
  private final DriverConfigurationProvider driverConfigurationProvider;
  private final Set<ConfigurationProvider> configurationProviders;
  private final AvroConfigurationSerializer configurationSerializer;

  @Inject
  private LocalRuntimeDriverConfigurationGenerator(final AvroConfigurationSerializer configurationSerializer,
                                           final REEFFileNames fileNames,
                                           final DriverConfigurationProvider driverConfigurationProvider,
                                           @Parameter(DriverConfigurationProviders.class)
                                           final Set<ConfigurationProvider> configurationProviders){
    this.fileNames = fileNames;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.configurationProviders = configurationProviders;
    this.configurationSerializer = configurationSerializer;
  }

  /**
   * Writes driver configuration to disk.
   * @param jobFolder The folder in which the job is staged.
   * @param jobId id of the job to be submitted
   * @param clientRemoteId
   * @return the configuration
   * @throws IOException
   */
  public Configuration writeConfiguration(final File jobFolder,
                                          final String jobId,
                                          final String clientRemoteId) throws IOException {
    final File driverFolder = new File(jobFolder, PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);

    final Configuration driverConfiguration1 = driverConfigurationProvider
        .getDriverConfiguration(jobFolder, clientRemoteId,
        jobId, Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER);
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
    return driverConfiguration;
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final File localAppSubmissionParametersFile = new File(args[0]);
    final File jobSubmissionParametersFile = new File(args[1]);

    if (!(jobSubmissionParametersFile.exists() && jobSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + jobSubmissionParametersFile.getAbsolutePath());
    }

    if (!(localAppSubmissionParametersFile.exists() && localAppSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + localAppSubmissionParametersFile.getAbsolutePath());
    }

    final LocalSubmissionFromCS localSubmission =
        LocalSubmissionFromCS.fromSubmissionParameterFiles(
            jobSubmissionParametersFile, localAppSubmissionParametersFile);

    LOG.log(Level.FINE, "Local driver config generation received from C#: {0}", localSubmission);
    final Configuration localRuntimeConfiguration = localSubmission.getRuntimeConfiguration();
    final LocalRuntimeDriverConfigurationGenerator localConfigurationGenerator = Tang.Factory.getTang()
        .newInjector(localRuntimeConfiguration)
        .getInstance(LocalRuntimeDriverConfigurationGenerator.class);
    localConfigurationGenerator.writeConfiguration(localSubmission.getJobFolder(),
        localSubmission.getJobId(), ClientRemoteIdentifier.NONE);
  }
}
