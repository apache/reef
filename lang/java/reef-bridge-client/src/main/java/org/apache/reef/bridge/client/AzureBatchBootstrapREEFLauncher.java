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

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.reef.bridge.client.avro.AvroAzureBatchJobSubmissionParameters;
import org.apache.reef.runtime.azbatch.AzureBatchClasspathProvider;
import org.apache.reef.runtime.azbatch.AzureBatchJVMPathProvider;
import org.apache.reef.runtime.azbatch.client.AzureBatchDriverConfigurationProviderImpl;
import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.azbatch.util.command.WindowsCommandBuilder;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortSet;
import org.apache.reef.wake.time.Clock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a bootstrap launcher for Azure Batch for submission from C#. It allows for Java Driver
 * configuration generation directly on the Driver without need of Java dependency if REST
 * submission is used. Note that the name of the class must contain "REEFLauncher" for the time
 * being in order for the Interop code to discover the class.
 */
@Interop(CppFiles = "DriverLauncher.cpp")
public final class AzureBatchBootstrapREEFLauncher {

  private static final Logger LOG = Logger.getLogger(AzureBatchBootstrapREEFLauncher.class.getName());
  private static final Tang TANG = Tang.Factory.getTang();

  public static void main(final String[] args) throws IOException, InjectionException {

    LOG.log(Level.INFO, "Entering BootstrapLauncher.main(). {0}", args[0]);

    if (args.length != 1) {

      final StringBuilder sb = new StringBuilder(
          "Bootstrap launcher should have one configuration file input," +
          " specifying the job submission parameters to be deserialized" +
          " to create the Azure Batch DriverConfiguration on the fly." +
          " Current args are [ ");
      for (String arg : args) {
        sb.append(arg).append(" ");
      }
      sb.append("]");

      final String message = sb.toString();
      throw fatal(message, new IllegalArgumentException(message));
    }

    final AvroAzureBatchJobSubmissionParameters jobSubmissionParameters =
        readAvroJobSubmissionParameters(new File(args[0]));
    final AzureBatchBootstrapDriverConfigGenerator azureBatchBootstrapDriverConfigGenerator =
        TANG.newInjector(generateConfiguration(jobSubmissionParameters))
            .getInstance(AzureBatchBootstrapDriverConfigGenerator.class);

    final LocalAddressProvider defaultLocalAddressProvider =
        Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);

    final JavaConfigurationBuilder launcherConfigBuilder =
        TANG.newConfigurationBuilder()
            .bindNamedParameter(RemoteConfiguration.ManagerName.class, "AzureBatchBootstrapREEFLauncher")
            .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
            .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
            .bindNamedParameter(RemoteConfiguration.HostAddress.class, defaultLocalAddressProvider.getLocalAddress())
            .bindSetEntry(Clock.RuntimeStartHandler.class, PIDStoreStartHandler.class);


    // Check if user has set up preferred ports to use.
    // If set, we prefer will launch driver that binds those ports.
    if (jobSubmissionParameters.getAzureBatchPoolDriverPortsList().size() > 0) {
      for (CharSequence port : jobSubmissionParameters.getAzureBatchPoolDriverPortsList()) {
        launcherConfigBuilder.bindSetEntry(TcpPortSet.class, port.toString());
      }
    }

    final Configuration launcherConfig = launcherConfigBuilder.build();

    try (final REEFEnvironment reef = REEFEnvironment.fromConfiguration(
        azureBatchBootstrapDriverConfigGenerator.getDriverConfigurationFromParams(
            jobSubmissionParameters), launcherConfig)) {
      reef.run();
    } catch (final InjectionException ex) {
      throw fatal("Unable to configure and start REEFEnvironment.", ex);
    }

    LOG.log(Level.INFO, "Exiting BootstrapLauncher.main()");

    System.exit(0); // TODO[REEF-1715]: Should be able to exit cleanly at the end of main()
  }

  private static AvroAzureBatchJobSubmissionParameters readAvroJobSubmissionParameters(
      final File paramsFile) throws IOException {
    final AvroAzureBatchJobSubmissionParameters avroAzureBatchJobSubmissionParameters;
    try (final FileInputStream fileInputStream = new FileInputStream(paramsFile)) {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          AvroAzureBatchJobSubmissionParameters.getClassSchema(), fileInputStream);
      final SpecificDatumReader<AvroAzureBatchJobSubmissionParameters> reader =
          new SpecificDatumReader<>(AvroAzureBatchJobSubmissionParameters.class);
      avroAzureBatchJobSubmissionParameters = reader.read(null, decoder);
    }
    return avroAzureBatchJobSubmissionParameters;
  }

  private static Configuration generateConfiguration(
      final AvroAzureBatchJobSubmissionParameters avroAzureBatchJobSubmissionParameters) {
    return TANG.newConfigurationBuilder()
        .bindImplementation(DriverConfigurationProvider.class, AzureBatchDriverConfigurationProviderImpl.class)
        .bindImplementation(RuntimeClasspathProvider.class, AzureBatchClasspathProvider.class)
        .bindImplementation(RuntimePathProvider.class, AzureBatchJVMPathProvider.class)
        .bindImplementation(CommandBuilder.class, WindowsCommandBuilder.class)
        .bindNamedParameter(AzureBatchAccountName.class,
            avroAzureBatchJobSubmissionParameters.getAzureBatchAccountName().toString())
        .bindNamedParameter(AzureBatchAccountUri.class,
            avroAzureBatchJobSubmissionParameters.getAzureBatchAccountUri().toString())
        .bindNamedParameter(AzureBatchPoolId.class,
            avroAzureBatchJobSubmissionParameters.getAzureBatchPoolId().toString())
        .bindNamedParameter(AzureStorageAccountName.class,
            avroAzureBatchJobSubmissionParameters.getAzureStorageAccountName().toString())
        .bindNamedParameter(AzureStorageContainerName.class,
            avroAzureBatchJobSubmissionParameters.getAzureStorageContainerName().toString())
        .bindNamedParameter(ContainerRegistryServer.class,
            avroAzureBatchJobSubmissionParameters.getContainerRegistryServer().toString())
        .bindNamedParameter(ContainerRegistryUsername.class,
            avroAzureBatchJobSubmissionParameters.getContainerRegistryUsername().toString())
        .bindNamedParameter(ContainerRegistryPassword.class,
            avroAzureBatchJobSubmissionParameters.getContainerRegistryPassword().toString())
        .bindNamedParameter(ContainerImageName.class,
            avroAzureBatchJobSubmissionParameters.getContainerImageName().toString())
        .build();
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private AzureBatchBootstrapREEFLauncher() {
  }
}
