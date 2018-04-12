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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.bridge.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.client.grpc.parameters.DriverServicePort;
import org.apache.reef.bridge.client.parameters.ClientDriverStopHandler;
import org.apache.reef.bridge.service.grpc.GRPCDriverService;
import org.apache.reef.bridge.service.parameters.*;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimePathProvider;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver Service Launcher - main class.
 */
public final class DriverServiceLauncher {

  public static final String ARG_SEPERATOR = ";";

  /**
   * Handler labels.
   */
  public final class HandlerLabels {

    public static final String START = "start";

    public static final String STOP = "stop";

    public static final String ALLOCATED_EVAL = "allocated-evaluator";

    public static final String COMPLETE_EVAL = "complete-evaluator";

    public static final String FAILED_EVAL = "failed-evaluator";

    public static final String ACTIVE_CXT = "active-context";

    public static final String CLOSED_CXT = "closed-context";

    public static final String MESSAGE_CXT = "context-message";

    public static final String FAILED_CXT = "failed-context";

    public static final String RUNNING_TASK = "running-task";

    public static final String FAILED_TASK = "failed-task";

    public static final String COMPLETED_TASK = "completed-task";

    public static final String SUSPENDED_TASK = "suspended-task";

    public static final String TASK_MESSAGE = "task-message";

    public static final String CLIENT_MESSAGE = "client-message";

    public static final String CLIENT_CLOSE = "client-close";

    public static final String CLIENT_CLOSE_WITH_MESSAGE = "client-close-with-message";

    public static final String HANDLER_LABEL_DESCRIPTION = "Handler Event Labels: \n" +
        "> " + START + "\n" +
        "> " + STOP + "\n" +
        "> " + ALLOCATED_EVAL + "\n" +
        "> " + COMPLETE_EVAL + "\n" +
        "> " + FAILED_EVAL + "\n" +
        "> " + ACTIVE_CXT + "\n" +
        "> " + CLOSED_CXT + "\n" +
        "> " + MESSAGE_CXT + "\n" +
        "> " + FAILED_CXT + "\n" +
        "> " + RUNNING_TASK + "\n" +
        "> " + FAILED_TASK + "\n" +
        "> " + COMPLETED_TASK + "\n" +
        "> " + SUSPENDED_TASK + "\n" +
        "> " + TASK_MESSAGE + "\n" +
        "> " + CLIENT_MESSAGE + "\n" +
        "> " + CLIENT_CLOSE + "\n" +
        "> " + CLIENT_CLOSE_WITH_MESSAGE + "\n" +
        "Specify a list of handler event labels seperated by '" +
        ARG_SEPERATOR + "'\n" +
        "e.g., \"" + START + ARG_SEPERATOR + STOP +
        "\" registers for the stop and start handlers, but none other.";

    private HandlerLabels() {}
  }

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
   * Parse the command line arguments.
   *
   * @param args command line arguments, as passed to main()
   * @return Configuration object.
   * @throws BindException configuration error.
   * @throws IOException   error reading the configuration.
   */
  private static Configuration parseCommandLine(final String[] args)
      throws BindException, IOException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(DriverClientHandlers.class);
    cl.registerShortNameOfClass(BridgeRuntime.class);
    cl.registerShortNameOfClass(BridgeJobId.class);
    cl.registerShortNameOfClass(DriverClientCommand.class);
    cl.registerShortNameOfClass(TcpPortRangeBegin.class);
    cl.registerShortNameOfClass(TcpPortRangeCount.class);
    cl.registerShortNameOfClass(TcpPortRangeTryCount.class);
    cl.registerShortNameOfClass(DriverClientFileDependencies.class);
    if (cl.processCommandLine(args) != null) {
      return confBuilder.build();
    } else {
      return null;
    }
  }

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param commandLineConf Parsed command line arguments, as passed into main().
   * @param runtime Which runtime to configure: local, yarn, azbatch
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   */
  private static Configuration getClientConfiguration(
      final Configuration commandLineConf, final String runtime)
      throws BindException {

    final Configuration runtimeConfiguration;

    if (RuntimeNames.LOCAL.equals(runtime)) {
      LOG.log(Level.FINE, "JavaBridge: Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .build();
    } else if (RuntimeNames.YARN.equals(runtime)){
      LOG.log(Level.FINE, "JavaBridge: Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    } else {
      throw new IllegalArgumentException("Unsupported runtime " + runtime);
    }

    return Configurations.merge(runtimeConfiguration, commandLineConf);
  }

  private static ConfigurationModule getDriverServiceConfigurationModule(
      final String jobId,
      final Set<String> handlerLabelSet,
      final List<String> fileDependencyList) {

    ConfigurationModule driverServiceConfigurationModule = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.DRIVER_SERVICE_IMPL, GRPCDriverService.class)
        .set(DriverConfiguration.LOCAL_LIBRARIES, EnvironmentUtils.getClassLocation(GRPCDriverService.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId);
    for (final String file : fileDependencyList) {
      driverServiceConfigurationModule.set(DriverConfiguration.LOCAL_FILES, file);
    }
    if (!handlerLabelSet.contains(HandlerLabels.START)) {
      throw new IllegalArgumentException("Start handler required");
    } else {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DriverServiceHandlers.StartHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STOP, DriverServiceHandlers.StopHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.ALLOCATED_EVAL)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverServiceHandlers.AllocatedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.COMPLETE_EVAL)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, DriverServiceHandlers.CompletedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_EVAL)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DriverServiceHandlers.FailedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.ACTIVE_CXT)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DriverServiceHandlers.ActiveContextHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLOSED_CXT)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_CLOSED, DriverServiceHandlers.ClosedContextHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_CXT)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_FAILED, DriverServiceHandlers.ContextFailedHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.MESSAGE_CXT)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DriverServiceHandlers.ContextMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.RUNNING_TASK)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_RUNNING, DriverServiceHandlers.RunningTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.COMPLETED_TASK)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_COMPLETED, DriverServiceHandlers.CompletedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_TASK)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_FAILED, DriverServiceHandlers.FailedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.TASK_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_MESSAGE, DriverServiceHandlers.TaskMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.SUSPENDED_TASK)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_TASK_SUSPENDED, DriverServiceHandlers.SuspendedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverServiceHandlers.ClientMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_CLOSE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverServiceHandlers.ClientCloseHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_CLOSE_WITH_MESSAGE)) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE, DriverServiceHandlers.ClientCloseWithMessageHandler.class);
    }
    return driverServiceConfigurationModule;
  }

  public static LauncherStatus submit(
      final String jobId,
      final Configuration runtimeConfiguration,
      final Configuration driverClientConfiguration,
      final List<String> fileDependencies,
      final List<String> libraryDependencies) throws InjectionException, IOException {

    final Injector runtimeInjector = Tang.Factory.getTang().newInjector(runtimeConfiguration);
    final Injector driverClientInjector = Tang.Factory.getTang().newInjector(driverClientConfiguration);
    driverClientInjector.bindVolatileParameter(DriverServicePort.class, 0);

    final Set<String> handlers = new HashSet<>();
    if (driverClientInjector.isParameterSet(DriverStartHandler.class)) {
      handlers.add(HandlerLabels.START);
    }
    if (driverClientInjector.isParameterSet(ClientDriverStopHandler.class)) {
      handlers.add(HandlerLabels.STOP);
    }
    if (driverClientInjector.isParameterSet(EvaluatorAllocatedHandlers.class)) {
      handlers.add(HandlerLabels.ALLOCATED_EVAL);
    }
    if (driverClientInjector.isParameterSet(EvaluatorCompletedHandlers.class)) {
      handlers.add(HandlerLabels.COMPLETE_EVAL);
    }
    if (driverClientInjector.isParameterSet(EvaluatorFailedHandlers.class)) {
      handlers.add(HandlerLabels.FAILED_EVAL);
    }
    if (driverClientInjector.isParameterSet(TaskRunningHandlers.class)) {
      handlers.add(HandlerLabels.RUNNING_TASK);
    }
    if (driverClientInjector.isParameterSet(TaskFailedHandlers.class)) {
      handlers.add(HandlerLabels.FAILED_TASK);
    }
    if (driverClientInjector.isParameterSet(TaskMessageHandlers.class)) {
      handlers.add(HandlerLabels.TASK_MESSAGE);
    }
    if (driverClientInjector.isParameterSet(TaskCompletedHandlers.class)) {
      handlers.add(HandlerLabels.COMPLETED_TASK);
    }
    if (driverClientInjector.isParameterSet(TaskSuspendedHandlers.class)) {
      handlers.add(HandlerLabels.SUSPENDED_TASK);
    }
    if (driverClientInjector.isParameterSet(ContextActiveHandlers.class)) {
      handlers.add(HandlerLabels.ACTIVE_CXT);
    }
    if (driverClientInjector.isParameterSet(ContextClosedHandlers.class)) {
      handlers.add(HandlerLabels.CLOSED_CXT);
    }
    if (driverClientInjector.isParameterSet(ContextMessageHandlers.class)) {
      handlers.add(HandlerLabels.MESSAGE_CXT);
    }
    if (driverClientInjector.isParameterSet(ContextFailedHandlers.class)) {
      handlers.add(HandlerLabels.FAILED_CXT);
    }
    if (driverClientInjector.isParameterSet(ClientMessageHandlers.class)) {
      handlers.add(HandlerLabels.CLIENT_MESSAGE);
    }
    if (driverClientInjector.isParameterSet(ClientCloseHandlers.class)) {
      handlers.add(HandlerLabels.CLIENT_CLOSE);
    }
    if (driverClientInjector.isParameterSet(ClientCloseWithMessageHandlers.class)) {
      handlers.add(HandlerLabels.CLIENT_CLOSE_WITH_MESSAGE);
    }
    final ConfigurationSerializer configurationSerializer =
        driverClientInjector.getInstance(ConfigurationSerializer.class);
    final File driverClientConfigurationFile = new File("driverclient.conf");
    configurationSerializer.toFile(driverClientConfiguration, driverClientConfigurationFile);

    ConfigurationModule driverServiceConfigurationModule =
        getDriverServiceConfigurationModule(jobId, handlers, fileDependencies);
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .set(DriverConfiguration.LOCAL_FILES, driverClientConfigurationFile.getAbsolutePath());
    for (final String library : libraryDependencies) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.GLOBAL_LIBRARIES, library);
    }

    final REEFFileNames fileNames = runtimeInjector.getInstance(REEFFileNames.class);
    final ClasspathProvider classpathProvider = runtimeInjector.getInstance(ClasspathProvider.class);
    final RuntimePathProvider runtimePathProvider = runtimeInjector.getInstance(RuntimePathProvider.class);
    // SET EXEC COMMAND
    final List<String> launchCommand = new JavaLaunchCommandBuilder(JavaDriverClientLauncher.class, null)
        .setConfigurationFilePaths(
            Collections.singletonList("./" + fileNames.getLocalFolderPath() + "/" +
                driverClientConfigurationFile.getName()))
        .setJavaPath(runtimePathProvider.getPath())
        .setClassPath(classpathProvider.getEvaluatorClasspath())
        .build();
    final String cmd = StringUtils.join(launchCommand, ' ');
    LOG.log(Level.INFO, "LAUNCH COMMAND: " + cmd);
    final Configuration driverServiceConfiguration =
        driverServiceConfigurationModule
            .set(DriverServiceConfiguration.DRIVER_CLIENT_COMMAND, cmd)
            .build();
    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverServiceConfiguration);
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {

    try {
      final Configuration commandLineConf = parseCommandLine(args);
      if (commandLineConf == null) {
        return;
      }
      final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
      final String handlerLabels = injector.getNamedInstance(DriverClientHandlers.class);
      final Set<String> handlerLabelSet =
          new HashSet<>(Lists.newArrayList(handlerLabels.split(ARG_SEPERATOR)));
      final String runtime = injector.getNamedInstance(BridgeRuntime.class);
      final int jobNum = injector.getNamedInstance(BridgeJobId.class);
      final String jobId = String.format("bridge.%d",
          jobNum < 0 ? System.currentTimeMillis() : jobNum);
      final String fileDependencies = injector.getNamedInstance(DriverClientFileDependencies.class);
      final List<String> fileDependencyList = Lists.newArrayList(fileDependencies.split(ARG_SEPERATOR));

      final Configuration runtimeConfig = getClientConfiguration(commandLineConf, runtime);
      final Configuration driverBridgeConfig =
          getDriverServiceConfigurationModule(jobId, handlerLabelSet, fileDependencyList).build();
      final Configuration submittedConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(driverBridgeConfig, commandLineConf).build();

      DriverLauncher.getLauncher(runtimeConfig).run(submittedConfiguration);

      LOG.log(Level.INFO, "JavaBridge: Stop Client {0}", jobId);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  // Named Parameters that are specific to this JavaDriverClientLauncher.

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "The handlers that should be configured. " +
      HandlerLabels.HANDLER_LABEL_DESCRIPTION,
      short_name = "handlers", default_value = "start")
  public final class DriverClientHandlers implements Name<String> {
  }

  /**
   * Driver client file dependencies.
   */
  @NamedParameter(doc = "list of file dependencies for driver client separated by '" + ARG_SEPERATOR + "'",
      short_name = "driver-client-files", default_value = "")
  public final class DriverClientFileDependencies implements Name<String> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "The runtime to use: local, yarn, azbatch",
      short_name = "runtime", default_value = "local")
  public final class BridgeRuntime implements Name<String> {
  }

  /**
   * Command line parameter = Numeric ID for the job.
   */
  @NamedParameter(doc = "Numeric ID for the job",
      short_name = "id", default_value = "-1")
  public final class BridgeJobId implements Name<Integer> {
  }
}
