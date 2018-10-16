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

package org.apache.reef.bridge.client.grpc;

import com.google.protobuf.util.JsonFormat;
import org.apache.reef.bridge.driver.launch.RuntimeConfigurationProvider;
import org.apache.reef.bridge.driver.launch.azbatch.AzureBatchConfigurationProvider;
import org.apache.reef.bridge.driver.launch.hdinsight.HDInsightConfigurationProvider;
import org.apache.reef.bridge.driver.launch.local.LocalConfigurationProvider;
import org.apache.reef.bridge.driver.launch.yarn.YarnConfigurationProvider;
import org.apache.reef.bridge.driver.service.DriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.service.grpc.GRPCDriverServiceConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client launcher.
 */
public final class ClientLauncher {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(ClientLauncher.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  private final RuntimeConfigurationProvider runtimeConfigurationProvider;

  private final DriverServiceConfigurationProvider driverServiceConfigurationProvider;

  /**
   * This class should not be instantiated.
   */
  @Inject
  private ClientLauncher(
      final RuntimeConfigurationProvider runtimeConfigurationProvider,
      final DriverServiceConfigurationProvider driverServiceConfigurationProvider) {
    this.runtimeConfigurationProvider = runtimeConfigurationProvider;
    this.driverServiceConfigurationProvider = driverServiceConfigurationProvider;
  }

  private LauncherStatus launch(final int port,
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto) {
    try {
      final Configuration runtimeConfiguration = Configurations.merge(
          getClientServiceConfiguration(port),
          this.runtimeConfigurationProvider.getRuntimeConfiguration(driverClientConfigurationProto));
      try (ClientService clientService = TANG.newInjector(runtimeConfiguration).getInstance(ClientService.class)) {
        return clientService.submit(
            this.driverServiceConfigurationProvider.getDriverServiceConfiguration(driverClientConfigurationProto));
      }
    } catch (final InjectionException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
      throw new RuntimeException("Could not launch driver service", ex);
    }
  }

  static LauncherStatus submit(final int port,
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration)
      throws InjectionException {
    final Configuration driverServiceLauncherConfiguration;
    switch (driverClientConfiguration.getRuntimeCase()) {
    case YARN_RUNTIME:
      driverServiceLauncherConfiguration = TANG.newConfigurationBuilder()
          .bindImplementation(DriverServiceConfigurationProvider.class,
              GRPCDriverServiceConfigurationProvider.class)
          .bind(RuntimeConfigurationProvider.class, YarnConfigurationProvider.class)
          .build();
      break;
    case LOCAL_RUNTIME:
      driverServiceLauncherConfiguration = TANG.newConfigurationBuilder()
          .bind(RuntimeConfigurationProvider.class, LocalConfigurationProvider.class)
          .bindImplementation(DriverServiceConfigurationProvider.class,
              GRPCDriverServiceConfigurationProvider.class)
          .build();
      break;
    case AZBATCH_RUNTIME:
      driverServiceLauncherConfiguration = TANG.newConfigurationBuilder()
          .bind(RuntimeConfigurationProvider.class, AzureBatchConfigurationProvider.class)
          .bindImplementation(DriverServiceConfigurationProvider.class,
              GRPCDriverServiceConfigurationProvider.class)
          .build();
      break;
    case HDI_RUNTIME:
      driverServiceLauncherConfiguration = TANG.newConfigurationBuilder()
          .bind(RuntimeConfigurationProvider.class, HDInsightConfigurationProvider.class)
          .bindImplementation(DriverServiceConfigurationProvider.class,
              GRPCDriverServiceConfigurationProvider.class)
          .build();
      break;
    default:
      throw new RuntimeException("unknown runtime " + driverClientConfiguration.getRuntimeCase());
    }
    return TANG.newInjector(driverServiceLauncherConfiguration).getInstance(ClientLauncher.class)
        .launch(port, driverClientConfiguration);
  }

  private static Configuration getClientServiceConfiguration(final int port) {
    return ClientServiceConfiguration.CONF
        .set(ClientServiceConfiguration.CLIENT_SERVER_PORT, port)
        .set(ClientConfiguration.ON_JOB_SUBMITTED, ClientService.SubmittedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_RUNNING, ClientService.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, ClientService.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, ClientService.FailedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, ClientService.JobMessageHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, ClientService.RuntimeErrorHandler.class)
        .set(ClientConfiguration.ON_WAKE_ERROR, ClientService.WakeErrorHandler.class)
        .build();
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) throws IOException, InjectionException {
    final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto;
    if (args.length > 0) {
      final String content = new String(Files.readAllBytes(Paths.get(args[0])));
      final ClientProtocol.DriverClientConfiguration.Builder driverClientConfigurationProtoBuilder =
          ClientProtocol.DriverClientConfiguration.newBuilder();
      JsonFormat.parser()
          .usingTypeRegistry(JsonFormat.TypeRegistry.getEmptyTypeRegistry())
          .merge(content, driverClientConfigurationProtoBuilder);
      driverClientConfigurationProto = driverClientConfigurationProtoBuilder.build();
      final int port = (args.length == 2) ? Integer.parseInt(args[1]) : 0;
      final LauncherStatus status = submit(port, driverClientConfigurationProto);
      LOG.log(Level.INFO, "Status: {0}", status);
    } else {
      LOG.log(Level.SEVERE, ClientLauncher.class.getName() +
          " accepts single required argument referencing a file that contains a " +
          "client protocol buffer driver configuration");
      System.exit(1);
    }
  }
}
