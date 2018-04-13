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
package org.apache.reef.bridge.service;

import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.bridge.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.examples.WindowsRuntimePathProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.bridge.service.grpc.GRPCDriverService;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.runtime.common.files.UnixJVMPathProvider;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.OSUtils;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
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

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param driverClientConfigurationProto containing which runtime to configure: local, yarn, azbatch
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   */
  private static Configuration getRuntimeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto)
      throws BindException {
    switch (driverClientConfigurationProto.getRuntimeCase()) {
    case LOCAL_RUNTIME:
      return getLocalRuntimeConfiguration(driverClientConfigurationProto);
    case YARN_RUNTIME:
      return getYarnRuntimeConfiguration(driverClientConfigurationProto);
    default:
      throw new IllegalArgumentException("Unsupported runtime " + driverClientConfigurationProto.getRuntimeCase());
    }
  }

  private static Configuration getLocalRuntimeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto)
      throws BindException {
    LOG.log(Level.FINE, "JavaBridge: Running on the local runtime");
    return LocalRuntimeConfiguration.CONF
        .build();
  }

  private static Configuration getYarnRuntimeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto)
      throws BindException {
    LOG.log(Level.FINE, "JavaBridge: Running on YARN");
    return YarnClientConfiguration.CONF.build();
  }

  private static Configuration getDriverServiceConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto) {
    // Set required parameters
    ConfigurationModule driverServiceConfigurationModule = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.DRIVER_SERVICE_IMPL, GRPCDriverService.class)
        .set(DriverServiceConfiguration.DRIVER_CLIENT_COMMAND,
            driverClientConfigurationProto.getDriverClientLaunchCommand())
        .set(DriverConfiguration.DRIVER_IDENTIFIER, driverClientConfigurationProto.getJobid());

    // Set file dependencies
    final List<String> localLibraries = new ArrayList<>();
    localLibraries.add(EnvironmentUtils.getClassLocation(GRPCDriverService.class));
    if (driverClientConfigurationProto.getLocalLibrariesCount() > 0) {
      localLibraries.addAll(driverClientConfigurationProto.getLocalLibrariesList());
    }
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.LOCAL_LIBRARIES, localLibraries);
    if (driverClientConfigurationProto.getGlobalLibrariesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES,
              driverClientConfigurationProto.getGlobalLibrariesList());
    }
    if (driverClientConfigurationProto.getLocalFilesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.LOCAL_FILES,
              driverClientConfigurationProto.getLocalFilesList());
    }
    if (driverClientConfigurationProto.getGlobalFilesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.GLOBAL_FILES,
              driverClientConfigurationProto.getGlobalFilesList());
    }
    // Setup driver resources
    if (driverClientConfigurationProto.getCpuCores() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_CPU_CORES, driverClientConfigurationProto.getCpuCores());
    }
    if (driverClientConfigurationProto.getMemoryMb() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_MEMORY, driverClientConfigurationProto.getMemoryMb());
    }

    // Setup handlers
    final Set<ClientProtocol.DriverClientConfiguration.Handlers> handlerLabelSet = new HashSet<>();
    handlerLabelSet.addAll(driverClientConfigurationProto.getHandlerList());
    if (!handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.START.START)) {
      throw new IllegalArgumentException("Start handler required");
    } else {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DriverServiceHandlers.StartHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STOP, DriverServiceHandlers.StopHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.EVALUATOR_ALLOCATED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverServiceHandlers.AllocatedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.EVALUATOR_COMPLETED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, DriverServiceHandlers.CompletedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.EVALUATOR_FAILED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DriverServiceHandlers.FailedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CONTEXT_ACTIVE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DriverServiceHandlers.ActiveContextHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CONTEXT_CLOSED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_CLOSED, DriverServiceHandlers.ClosedContextHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CONTEXT_FAILED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_FAILED, DriverServiceHandlers.ContextFailedHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CONTEXT_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DriverServiceHandlers.ContextMessageHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.TASK_RUNNING)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_RUNNING, DriverServiceHandlers.RunningTaskHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.TASK_COMPLETED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_COMPLETED, DriverServiceHandlers.CompletedTaskHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.TASK_FAILED)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_FAILED, DriverServiceHandlers.FailedTaskHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.TASK_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_MESSAGE, DriverServiceHandlers.TaskMessageHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CLIENT_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverServiceHandlers.ClientMessageHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CLIENT_CLOSE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverServiceHandlers.ClientCloseHandler.class);
    }
    if (handlerLabelSet.contains(ClientProtocol.DriverClientConfiguration.Handlers.CLIENT_CLOSE_WITH_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE, DriverServiceHandlers.ClientCloseWithMessageHandler.class);
    }

    return setTcpPortRange(driverClientConfigurationProto, driverServiceConfigurationModule.build());
  }

  private static Configuration setTcpPortRange(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto,
      final Configuration driverServiceConfiguration) {
    JavaConfigurationBuilder configurationModuleBuilder =
        Tang.Factory.getTang().newConfigurationBuilder(driverServiceConfiguration)
            .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class);
    // Setup TCP constraints
    if (driverClientConfigurationProto.getTcpPortRangeBegin() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeBegin.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeBegin()));
    }
    if (driverClientConfigurationProto.getTcpPortRangeCount() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeCount.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeCount()));
    }
    if (driverClientConfigurationProto.getTcpPortRangeTryCount() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeCount.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeTryCount()));
    }
    return configurationModuleBuilder.build();
  }

  public static LauncherStatus submit(
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

      // Get runtime injector and piece together the launch command based on its classpath info
      final Configuration runtimeConfiguration = getRuntimeConfiguration(driverClientConfigurationProto);
      // Resolve OS Runtime Path Provider
      final Configuration runtimeOSConfiguration = Configurations.merge(
          Tang.Factory.getTang().newConfigurationBuilder()
              .bind(RuntimePathProvider.class,
                  OSUtils.isWindows() ? WindowsRuntimePathProvider.class : UnixJVMPathProvider.class)
              .build(),
          runtimeConfiguration);
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



      // Configure driver service and launch the job
      final Configuration driverServiceConfiguration = getDriverServiceConfiguration(builder.build());
      return DriverLauncher.getLauncher(runtimeOSConfiguration).run(driverServiceConfiguration);
    } finally {
      driverClientConfigurationFile.delete();
    }
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

      final Configuration runtimeConfig = getRuntimeConfiguration(driverClientConfigurationProto);
      final Configuration driverConfig = getDriverServiceConfiguration(driverClientConfigurationProto);
      DriverLauncher.getLauncher(runtimeConfig).run(driverConfig);
      LOG.log(Level.INFO, "JavaBridge: Stop Client {0}", driverClientConfigurationProto.getJobid());
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
