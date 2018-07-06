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

import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.bridge.driver.launch.BridgeDriverLauncher;
import org.apache.reef.bridge.driver.launch.azbatch.AzureBatchLauncher;
import org.apache.reef.bridge.driver.launch.local.LocalLauncher;
import org.apache.reef.bridge.driver.launch.yarn.YarnLauncher;
import org.apache.reef.bridge.driver.service.DriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.service.grpc.GRPCDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.azbatch.AzureBatchClasspathProvider;
import org.apache.reef.runtime.common.files.*;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver Service Launcher - main class.
 */
@ClientSide
@Unstable
public final class DriverServiceLauncher {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(DriverServiceLauncher.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  /**
   * This class should not be instantiated.
   */
  private DriverServiceLauncher() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Submit a new REEF driver service (job).
   * @param driverClientConfigurationProto client configuration protocol buffer
   * @param driverClientConfiguration driver configuration
   * @return LauncherStatus
   * @throws InjectionException
   * @throws IOException
   */
  public static LauncherStatus submit(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto,
      final Configuration driverClientConfiguration)
      throws InjectionException, IOException {
    ClientProtocol.DriverClientConfiguration.Builder builder =
        ClientProtocol.DriverClientConfiguration.newBuilder(driverClientConfigurationProto);
    final File driverClientConfigurationFile = File.createTempFile("driverclient", ".conf");
    try {
      // Write driver client configuration to a file
      final Injector driverClientInjector = TANG.newInjector(driverClientConfiguration);
      final ConfigurationSerializer configurationSerializer =
          driverClientInjector.getInstance(ConfigurationSerializer.class);
      configurationSerializer.toFile(driverClientConfiguration, driverClientConfigurationFile);

      // Resolve Runtime ClassPath Provider.
      final Configuration runtimeClassPathProvider;
      switch (driverClientConfigurationProto.getRuntimeCase()) {
      case YARN_RUNTIME:
        runtimeClassPathProvider = TANG.newConfigurationBuilder()
            .bind(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
            .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class,
                YarnConfigurationConstructor.class)
            .build();
        break;
      case LOCAL_RUNTIME:
        runtimeClassPathProvider = TANG.newConfigurationBuilder()
            .bind(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
            .build();
        break;
      case AZBATCH_RUNTIME:
        runtimeClassPathProvider = TANG.newConfigurationBuilder()
            .bind(RuntimeClasspathProvider.class, AzureBatchClasspathProvider.class)
            .build();
        break;
      default:
        throw new RuntimeException("unknown runtime " + driverClientConfigurationProto.getRuntimeCase());
      }
      final Injector runtimeInjector = TANG.newInjector(runtimeClassPathProvider);
      final REEFFileNames fileNames = runtimeInjector.getInstance(REEFFileNames.class);
      final ClasspathProvider classpathProvider = runtimeInjector.getInstance(ClasspathProvider.class);
      final List<String> launchCommand = new JavaLaunchCommandBuilder(JavaDriverClientLauncher.class, null)
          .setConfigurationFilePaths(
              Collections.singletonList("./" + fileNames.getLocalFolderPath() + "/" +
                  driverClientConfigurationFile.getName()))
          .setJavaPath("java")
          .setClassPath(driverClientConfigurationProto.getOperatingSystem() ==
              ClientProtocol.DriverClientConfiguration.OS.WINDOWS ?
              StringUtils.join(classpathProvider.getDriverClasspath(), ";") :
              StringUtils.join(classpathProvider.getDriverClasspath(), ":"))
          .build();
      final String cmd = StringUtils.join(launchCommand, ' ');
      builder.setDriverClientLaunchCommand(cmd);
      builder.addLocalFiles(driverClientConfigurationFile.getAbsolutePath());

      return launch(driverClientConfigurationProto);
    } finally {
      driverClientConfigurationFile.deleteOnExit();
    }
  }

  private static LauncherStatus launch(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto) throws InjectionException {

    final ClientProtocol.DriverClientConfiguration.RuntimeCase runtime =
        driverClientConfigurationProto.getRuntimeCase();

    final Class<? extends BridgeDriverLauncher> launcherClass;
    switch (runtime) {
    case YARN_RUNTIME:
      launcherClass = YarnLauncher.class;
      break;
    case LOCAL_RUNTIME:
      launcherClass = LocalLauncher.class;
      break;
    case AZBATCH_RUNTIME:
      launcherClass = AzureBatchLauncher.class;
      break;
    default:
      throw new RuntimeException("Unknown runtime: " + runtime);
    }
    final Configuration jobSubmissionClientConfig = TANG.newConfigurationBuilder()
        .bindImplementation(BridgeDriverLauncher.class, launcherClass)
        .bindImplementation(DriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .build();
    final BridgeDriverLauncher driverServiceLauncher =
        TANG.newInjector(jobSubmissionClientConfig).getInstance(launcherClass);
    return driverServiceLauncher.launch(driverClientConfigurationProto);
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) throws IOException, InjectionException {
    if (args.length != 1) {
      LOG.log(Level.SEVERE,
          "Expected a single command line argument with a file containing client protobuf driver configuration");
      System.exit(1);
    }
    final ClientProtocol.DriverClientConfiguration.Builder driverClientConfigurationProtoBuilder =
        ClientProtocol.DriverClientConfiguration.newBuilder();
    try (final Reader reader = new FileReader(args[0])) {
      JsonFormat.parser()
          .usingTypeRegistry(JsonFormat.TypeRegistry.getEmptyTypeRegistry())
          .merge(reader, driverClientConfigurationProtoBuilder);
    }
    final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto =
        driverClientConfigurationProtoBuilder.build();
    final LauncherStatus status = launch(driverClientConfigurationProto);
    LOG.log(Level.INFO, "Status: {0}", status);
  }
}
