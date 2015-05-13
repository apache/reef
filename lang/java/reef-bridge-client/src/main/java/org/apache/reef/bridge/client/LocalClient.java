/**
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

import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.client.DriverConfigurationProvider;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.local.client.PreparedDriverFolderLauncher;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.wake.remote.ports.RangeTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;

/**
 * Submits a folder containing a Driver to the local runtime.
 */
public class LocalClient {

  private static final String CLIENT_REMOTE_ID = AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.NONE;
  private final AvroConfigurationSerializer configurationSerializer;
  private final PreparedDriverFolderLauncher launcher;
  private final REEFFileNames fileNames;
  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  public LocalClient(final AvroConfigurationSerializer configurationSerializer,
                     final PreparedDriverFolderLauncher launcher,
                     final REEFFileNames fileNames,
                     final DriverConfigurationProvider driverConfigurationProvider) {
    this.configurationSerializer = configurationSerializer;
    this.launcher = launcher;
    this.fileNames = fileNames;
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  public void submit(final File jobFolder, final String jobId) throws IOException {
    if (!jobFolder.exists()) {
      throw new IOException("The Job folder" + jobFolder.getAbsolutePath() + "doesn't exist.");
    }

    final File driverFolder = new File(jobFolder, PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);
    if (!driverFolder.exists()) {
      throw new IOException("The Driver folder " + driverFolder.getAbsolutePath() + " doesn't exist.");
    }

    final Configuration driverConfiguration = driverConfigurationProvider
        .getDriverConfiguration(jobFolder, CLIENT_REMOTE_ID, jobId, Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER);
    final File driverConfigurationFile = new File(driverFolder, fileNames.getDriverConfigurationPath());
    configurationSerializer.toFile(driverConfiguration, driverConfigurationFile);
    launcher.launch(driverFolder, jobId, CLIENT_REMOTE_ID);
  }


  public static void main(final String[] args) throws InjectionException, IOException {
    // TODO: Make the parameters of the local runtime command line arguments of this tool.

    // We assume the given path to be the one of the driver. The job folder is one level up from there.
    final File jobFolder = new File(args[0]).getParentFile();
    // The job identifier
    final String jobId = args[1];
    // The number of evaluators the local runtime can create
    final int numberOfEvaluators = Integer.valueOf(args[2]);

    final Configuration runtimeConfiguration = getRuntimeConfiguration(args);

    final LocalClient client = Tang.Factory.getTang()
        .newInjector(runtimeConfiguration)
        .getInstance(LocalClient.class);

    client.submit(jobFolder, jobId);
  }
  private static Configuration getRuntimeConfiguration(String[] args){
    final ConfigurationModule runtimeConfigurationModule  = getRuntimeConfigurationModule(args);
    if (args.length <= 2){
      return runtimeConfigurationModule.build();
    }
    else {
      return runtimeConfigurationModule
          .set(LocalRuntimeConfiguration.TCP_PORT_PROVIDER, RangeTcpPortProvider.class)
          .set(LocalRuntimeConfiguration.DRIVER_CONFIGURATION_PROVIDERS, TcpPortConfigurationProvider.class)
          .set(LocalRuntimeConfiguration.TCP_PORT_RANGE_START, args[3])
          .set(LocalRuntimeConfiguration.TCP_PORT_RANGE_COUNT, args[4])
          .set(LocalRuntimeConfiguration.TCP_PORT_RANGE_TRY_COUNT, args[5])
          .build();
    }
  }
  private static ConfigurationModule getRuntimeConfigurationModule(String[] args){
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, args[2])
          .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, new File(args[0]).getParentFile().getParentFile().getAbsolutePath());
  }
}
