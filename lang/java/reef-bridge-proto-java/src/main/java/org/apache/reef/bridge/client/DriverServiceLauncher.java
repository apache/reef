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
import org.apache.reef.bridge.client.launch.LocalDriverServiceRuntimeLauncher;
import org.apache.reef.bridge.client.launch.YarnDriverServiceRuntimeLauncher;
import org.apache.reef.bridge.client.runtime.LocalDriverRuntimeConfigurationProvider;
import org.apache.reef.bridge.client.runtime.YarnDriverRuntimeConfigurationProvider;
import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.service.grpc.GRPCDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.proto.ClientProtocol;
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
import org.apache.reef.util.OSUtils;

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
    final File driverClientConfigurationFile = new File("driverclient.conf");
    try {
      // Write driver client configuration to a file
      final Injector driverClientInjector = Tang.Factory.getTang().newInjector(driverClientConfiguration);
      final ConfigurationSerializer configurationSerializer =
          driverClientInjector.getInstance(ConfigurationSerializer.class);
      configurationSerializer.toFile(driverClientConfiguration, driverClientConfigurationFile);

      // Resolve OS Runtime Path Provider.
      final Configuration runtimeOSConfiguration =
          driverClientConfigurationProto.getRuntimeCase() ==
              ClientProtocol.DriverClientConfiguration.RuntimeCase.YARN_RUNTIME ?
              Tang.Factory.getTang().newConfigurationBuilder()
                  .bind(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
                  .build() :
              Tang.Factory.getTang().newConfigurationBuilder()
                  .bind(RuntimePathProvider.class,
                      OSUtils.isWindows() ? WindowsRuntimePathProvider.class : UnixJVMPathProvider.class)
                  .bind(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
                  .build();
      final Injector runtimeInjector = Tang.Factory.getTang().newInjector(runtimeOSConfiguration);
      final REEFFileNames fileNames = runtimeInjector.getInstance(REEFFileNames.class);
      final ClasspathProvider classpathProvider = runtimeInjector.getInstance(ClasspathProvider.class);
      final RuntimePathProvider runtimePathProvider = runtimeInjector.getInstance(RuntimePathProvider.class);
      final List<String> launchCommand = new JavaLaunchCommandBuilder(JavaDriverClientLauncher.class, null)
          .setConfigurationFilePaths(
              Collections.singletonList("./" + fileNames.getLocalFolderPath() + "/" +
                  driverClientConfigurationFile.getName()))
          .setJavaPath(runtimePathProvider.getPath())
          .setClassPath(classpathProvider.getEvaluatorClasspath())
          .build();
      final String cmd = StringUtils.join(launchCommand, ' ');
      builder.setDriverClientLaunchCommand(cmd);
      builder.addLocalFiles(driverClientConfigurationFile.getAbsolutePath());

      // call main()
      final File driverClientConfFile = new File("driverclient.json");
      try {
        try (PrintWriter out = new PrintWriter(driverClientConfFile)) {
          out.println(JsonFormat.printer().print(builder.build()));
        }
        main(new String[]{driverClientConfFile.getAbsolutePath()});
      } finally {
        driverClientConfFile.delete();
      }
    } finally {
      driverClientConfigurationFile.delete();
    }
  }

  private static IDriverServiceRuntimeLauncher getLocalDriverServiceLauncher() throws InjectionException {
    final Configuration localJobSubmissionClientConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IDriverRuntimeConfigurationProvider.class,
            LocalDriverRuntimeConfigurationProvider.class)
        .bindImplementation(IDriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
        .build();
    return Tang.Factory.getTang()
        .newInjector(localJobSubmissionClientConfig).getInstance(LocalDriverServiceRuntimeLauncher.class);
  }


  private static IDriverServiceRuntimeLauncher getYarnDriverServiceLauncher() throws InjectionException {
    final Configuration yarnJobSubmissionClientConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IDriverRuntimeConfigurationProvider.class,
            YarnDriverRuntimeConfigurationProvider.class)
        .bindImplementation(IDriverServiceConfigurationProvider.class,
            GRPCDriverServiceConfigurationProvider.class)
        .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
        .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
        .build();
    return Tang.Factory.getTang()
        .newInjector(yarnJobSubmissionClientConfig).getInstance(YarnDriverServiceRuntimeLauncher.class);
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
        final IDriverServiceRuntimeLauncher yarnDriverServiceLauncher = getYarnDriverServiceLauncher();
        yarnDriverServiceLauncher.launch(driverClientConfigurationProto);
        break;
      case LOCAL_RUNTIME:
        final IDriverServiceRuntimeLauncher localDriverServiceLauncher = getLocalDriverServiceLauncher();
        localDriverServiceLauncher.launch(driverClientConfigurationProto);
        break;
      default:
      }
      LOG.log(Level.INFO, "JavaBridge: Stop Client {0}", driverClientConfigurationProto.getJobid());
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
