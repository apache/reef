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
import org.apache.reef.bridge.driver.launch.IDriverLauncher;
import org.apache.reef.bridge.driver.launch.azbatch.AzureBatchLauncher;
import org.apache.reef.bridge.driver.launch.local.LocalLauncher;
import org.apache.reef.bridge.driver.launch.yarn.YarnLauncher;
import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.service.grpc.GRPCDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.runtime.azbatch.AzureBatchClasspathProvider;
import org.apache.reef.runtime.common.files.*;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver Service Launcher - main class.
 */
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

  public static void submit(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto,
      final Configuration driverClientConfiguration)
      throws InjectionException, IOException {
    ClientProtocol.DriverClientConfiguration.Builder builder =
        ClientProtocol.DriverClientConfiguration.newBuilder(driverClientConfigurationProto);
    final File driverClientConfigurationFile = File.createTempFile("driverclient", ".conf");
    try {
      // Write driver client configuration to a file
      final Injector driverClientInjector = Tang.Factory.getTang().newInjector(driverClientConfiguration);
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

      // call main()
      final File driverClientConfFile = File.createTempFile("driverclient", ".json");
      try {
        try (PrintWriter out = new PrintWriter(driverClientConfFile)) {
          out.println(JsonFormat.printer().print(builder.build()));
        }
        main(new String[]{driverClientConfFile.getAbsolutePath()});
      } finally {
        driverClientConfFile.deleteOnExit();
      }
    } finally {
      driverClientConfigurationFile.deleteOnExit();
    }
  }

  private static IDriverLauncher getLocalDriverServiceLauncher() throws InjectionException {
    final Configuration localJobSubmissionClientConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IDriverLauncher.class, LocalLauncher.class)
        .bindImplementation(IDriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .build();
    return Tang.Factory.getTang()
        .newInjector(localJobSubmissionClientConfig).getInstance(LocalLauncher.class);
  }


  private static IDriverLauncher getYarnDriverServiceLauncher() throws InjectionException {
    final Configuration yarnJobSubmissionClientConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IDriverLauncher.class, YarnLauncher.class)
        .bindImplementation(IDriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .build();
    return Tang.Factory.getTang()
        .newInjector(yarnJobSubmissionClientConfig).getInstance(YarnLauncher.class);
  }

  private static IDriverLauncher getAzureBatchDriverServiceLauncher() throws  InjectionException {
    final Configuration azbatchJobSubmissionClientConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IDriverLauncher.class, AzureBatchLauncher.class)
        .bindImplementation(IDriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .build();
    return Tang.Factory.getTang().newInjector(azbatchJobSubmissionClientConfig).getInstance(AzureBatchLauncher.class);
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      if (args.length != 1) {
        LOG.log(Level.SEVERE, DriverServiceLauncher.class.getName() +
            " accepts single argument referencing a file that contains a client protocol buffer driver configuration");
      }
      final String content;
      try {
        content = new String(Files.readAllBytes(Paths.get(args[0])));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      final ClientProtocol.DriverClientConfiguration.Builder driverClientConfigurationProtoBuilder =
          ClientProtocol.DriverClientConfiguration.newBuilder();
      JsonFormat.parser()
          .usingTypeRegistry(JsonFormat.TypeRegistry.getEmptyTypeRegistry())
          .merge(content, driverClientConfigurationProtoBuilder);
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto =
          driverClientConfigurationProtoBuilder.build();
      switch (driverClientConfigurationProto.getRuntimeCase()) {
      case YARN_RUNTIME:
        final IDriverLauncher yarnDriverServiceLauncher = getYarnDriverServiceLauncher();
        yarnDriverServiceLauncher.launch(driverClientConfigurationProto);
        break;
      case LOCAL_RUNTIME:
        final IDriverLauncher localDriverServiceLauncher = getLocalDriverServiceLauncher();
        localDriverServiceLauncher.launch(driverClientConfigurationProto);
        break;
      case AZBATCH_RUNTIME:
        final IDriverLauncher azureBatchDriverServiceLauncher = getAzureBatchDriverServiceLauncher();
        azureBatchDriverServiceLauncher.launch(driverClientConfigurationProto);
        break;
      default:
      }
      LOG.log(Level.INFO, "JavaBridge: Stop Client {0}", driverClientConfigurationProto.getJobid());
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
