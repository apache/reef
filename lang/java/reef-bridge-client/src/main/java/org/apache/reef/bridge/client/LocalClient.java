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
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.client.DriverConfigurationProvider;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.local.client.PreparedDriverFolderLauncher;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Submits a folder containing a Driver to the local runtime.
 */
public class LocalClient {

  private static final String CLIENT_REMOTE_ID = ClientRemoteIdentifier.NONE;
  private final AvroConfigurationSerializer configurationSerializer;
  private final PreparedDriverFolderLauncher launcher;
  private final REEFFileNames fileNames;
  private final DriverConfigurationProvider driverConfigurationProvider;
  private final Set<ConfigurationProvider> configurationProviders;

  @Inject
  public LocalClient(final AvroConfigurationSerializer configurationSerializer,
                     final PreparedDriverFolderLauncher launcher,
                     final REEFFileNames fileNames,
                     final DriverConfigurationProvider driverConfigurationProvider,
                     @Parameter(DriverConfigurationProviders.class)
                     final Set<ConfigurationProvider> configurationProviders)  {
    this.configurationSerializer = configurationSerializer;
    this.launcher = launcher;
    this.fileNames = fileNames;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.configurationProviders = configurationProviders;
  }

  public void submit(final File jobFolder, final String jobId) throws IOException {
    if (!jobFolder.exists()) {
      throw new IOException("The Job folder" + jobFolder.getAbsolutePath() + "doesn't exist.");
    }

    final File driverFolder = new File(jobFolder, PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);
    if (!driverFolder.exists()) {
      throw new IOException("The Driver folder " + driverFolder.getAbsolutePath() + " doesn't exist.");
    }

    final Configuration driverConfiguration1 = driverConfigurationProvider
        .getDriverConfiguration(jobFolder, CLIENT_REMOTE_ID, jobId,
            Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER);
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final ConfigurationProvider configurationProvider : this.configurationProviders) {
      configurationBuilder.addConfiguration(configurationProvider.getConfiguration());
    }
    final Configuration providedConfigurations =  configurationBuilder.build();
    final Configuration driverConfiguration = Configurations.merge(
        driverConfiguration1,
        providedConfigurations);

    final File driverConfigurationFile = new File(driverFolder, fileNames.getDriverConfigurationPath());
    configurationSerializer.toFile(driverConfiguration, driverConfigurationFile);
    launcher.launch(driverFolder, jobId, CLIENT_REMOTE_ID);
  }


  public static void main(final String[] args) throws InjectionException, IOException {
    // TODO: Make the parameters of the local runtime command line arguments of this tool.

    // We assume the given path to be the one of the driver. The job folder is one level up from there.
    final File jobFolder = new File(args[0]).getParentFile();
    final String runtimeRootFolder = jobFolder.getParentFile().getAbsolutePath();
    final String jobId = args[1];
    // The number of evaluators the local runtime can create
    final int numberOfEvaluators = Integer.valueOf(args[2]);
    final int tcpBeginPort = Integer.valueOf(args[3]);
    final int tcpRangeCount = Integer.valueOf(args[4]);
    final int tcpTryCount = Integer.valueOf(args[5]);


    final Configuration runtimeConfiguration = getRuntimeConfiguration(numberOfEvaluators, runtimeRootFolder,
        tcpBeginPort, tcpRangeCount, tcpTryCount);

    final LocalClient client = Tang.Factory.getTang()
        .newInjector(runtimeConfiguration)
        .getInstance(LocalClient.class);

    client.submit(jobFolder, jobId);
  }

  private static Configuration getRuntimeConfiguration(
      int numberOfEvaluators,
      String runtimeRootFolder,
      int tcpBeginPort,
      int tcpRangeCount,
      int tcpTryCount) {
    final Configuration runtimeConfiguration = getRuntimeConfiguration(numberOfEvaluators, runtimeRootFolder);
    final Configuration userproviderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .build();
    return Configurations.merge(runtimeConfiguration, userproviderConfiguration);
  }

  private static Configuration getRuntimeConfiguration(int numberOfEvaluators, String runtimeRootFolder) {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, Integer.toString(numberOfEvaluators))
        .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, runtimeRootFolder)
        .build();
  }
}
