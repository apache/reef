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
package org.apache.reef.bridge;

import com.google.common.collect.Lists;
import org.apache.reef.bridge.grpc.GRPCDriverBridgeService;
import org.apache.reef.bridge.parameters.BridgeDriverProcessCommand;
import org.apache.reef.bridge.parameters.DriverServicePortRangeCount;
import org.apache.reef.bridge.parameters.DriverServiceStartPort;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pool of Evaluators example - main class.
 */
public final class Launch {
  /**
   * List of supported runtimes.
   */
  private static final class Runtimes {
    private static final String LOCAL = "local";

    private static final String YARN = "yarn";

    private static final String AZBATCH = "azbatch";
  }

  /**
   * List of handlers that can be registered by clients.
   */
  private static final class HandlerLabels {
    private static final String START = "start";

    private static final String STOP = "stop";

    private static final String ALLOCATED_EVAL = "allocated-evaluator";

    private static final String COMPLETE_EVAL = "complete-evaluator";

    private static final String FAILED_EVAL = "failed-evaluator";

    private static final String ACTIVE_CXT = "active-context";

    private static final String CLOSED_CXT = "closed-context";

    private static final String MESSAGE_CXT = "context-message";

    private static final String FAILED_CXT = "failed-context";

    private static final String RUNNING_TASK = "running-task";

    private static final String FAILED_TASK = "failed-task";

    private static final String COMPLETED_TASK = "completed-task";

    private static final String SUSPENDED_TASK = "suspended-task";

    private static final String TASK_MESSAGE = "task-message";

    private static final String CLIENT_MESSAGE = "client-message";

    private static final String CLIENT_CLOSE = "client-close";

    private static final String CLIENT_CLOSE_WITH_MESSAGE = "client-close-with-message";
  }

  private static final String HANDLER_LABEL_SEPERATOR = ";";

  private static final String HANDLER_LABEL_DESCRIPTION = "Handler Event Labels: \n" +
      "> " + HandlerLabels.START + "\n" +
      "> " + HandlerLabels.STOP + "\n" +
      "> " + HandlerLabels.ALLOCATED_EVAL + "\n" +
      "> " + HandlerLabels.COMPLETE_EVAL + "\n" +
      "> " + HandlerLabels.FAILED_EVAL + "\n" +
      "> " + HandlerLabels.ACTIVE_CXT + "\n" +
      "> " + HandlerLabels.CLOSED_CXT + "\n" +
      "> " + HandlerLabels.MESSAGE_CXT + "\n" +
      "> " + HandlerLabels.FAILED_CXT + "\n" +
      "> " + HandlerLabels.RUNNING_TASK + "\n" +
      "> " + HandlerLabels.FAILED_TASK + "\n" +
      "> " + HandlerLabels.COMPLETED_TASK + "\n" +
      "> " + HandlerLabels.SUSPENDED_TASK + "\n" +
      "> " + HandlerLabels.TASK_MESSAGE + "\n" +
      "> " + HandlerLabels.CLIENT_MESSAGE + "\n" +
      "> " + HandlerLabels.CLIENT_CLOSE + "\n" +
      "> " + HandlerLabels.CLIENT_CLOSE_WITH_MESSAGE + "\n" +
      "Specify a list of handler event labels seperated by '" +
      HANDLER_LABEL_SEPERATOR + "'\n" +
      "e.g., \"" + HandlerLabels.START + HANDLER_LABEL_SEPERATOR + HandlerLabels.STOP +
      "\" registers for the stop and start handlers, but none other.";

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * This class should not be instantiated.
   */
  private Launch() {
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
    cl.registerShortNameOfClass(Handlers.class);
    cl.registerShortNameOfClass(Runtime.class);
    cl.registerShortNameOfClass(JobId.class);
    cl.registerShortNameOfClass(BridgeDriverProcessCommand.class);
    cl.registerShortNameOfClass(DriverServiceStartPort.class);
    cl.registerShortNameOfClass(DriverServicePortRangeCount.class);
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
      throws BindException, InjectionException {

    final Configuration runtimeConfiguration;

    if (Runtimes.LOCAL.equals(runtime)) {
      LOG.log(Level.FINE, "JavaBridge: Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .build();
    } else if (Runtimes.YARN.equals(runtime)){
      LOG.log(Level.FINE, "JavaBridge: Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    } else {
      throw new IllegalArgumentException("Unsupported runtime " + runtime);
    }

    return Configurations.merge(runtimeConfiguration, commandLineConf);
  }

  private static Configuration getDriverConfiguration(
      final String jobId,
      final Set<String> handlerLabelSet) {

    final ConfigurationModule driverBridgeConfigModule = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(GRPCDriverBridgeService.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId);
    if (!handlerLabelSet.contains(HandlerLabels.START)) {
      throw new IllegalArgumentException("Start handler required");
    } else {
      driverBridgeConfigModule.set(DriverConfiguration.ON_DRIVER_STARTED,
          DriverBridgeServiceHandlers.StartHandler.class);
      /* Stop handler not required, but set it for bridge shutdown */
      driverBridgeConfigModule.set(DriverConfiguration.ON_DRIVER_STOP,
          DriverBridgeServiceHandlers.StopHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.ALLOCATED_EVAL)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
          DriverBridgeServiceHandlers.AllocatedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.COMPLETE_EVAL)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_EVALUATOR_COMPLETED,
          DriverBridgeServiceHandlers.CompletedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_EVAL)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_EVALUATOR_FAILED,
          DriverBridgeServiceHandlers.FailedEvaluatorHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.ACTIVE_CXT)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CONTEXT_ACTIVE,
          DriverBridgeServiceHandlers.ActiveContextHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLOSED_CXT)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CONTEXT_CLOSED,
          DriverBridgeServiceHandlers.ClosedContextHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_CXT)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CONTEXT_FAILED,
          DriverBridgeServiceHandlers.ContextFailedHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.MESSAGE_CXT)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CONTEXT_MESSAGE,
          DriverBridgeServiceHandlers.ContextMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.RUNNING_TASK)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_TASK_RUNNING,
          DriverBridgeServiceHandlers.RunningTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.COMPLETED_TASK)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_TASK_COMPLETED,
          DriverBridgeServiceHandlers.CompletedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.FAILED_TASK)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_TASK_FAILED,
          DriverBridgeServiceHandlers.FailedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.TASK_MESSAGE)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_TASK_MESSAGE,
          DriverBridgeServiceHandlers.TaskMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.SUSPENDED_TASK)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_TASK_SUSPENDED,
          DriverBridgeServiceHandlers.SuspendedTaskHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_MESSAGE)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CLIENT_MESSAGE,
          DriverBridgeServiceHandlers.ClientMessageHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_CLOSE)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CLIENT_CLOSED,
          DriverBridgeServiceHandlers.ClientCloseHandler.class);
    }
    if (handlerLabelSet.contains(HandlerLabels.CLIENT_CLOSE_WITH_MESSAGE)) {
      driverBridgeConfigModule.set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE,
          DriverBridgeServiceHandlers.ClientCloseWithMessageHandler.class);
    }
    return driverBridgeConfigModule.build();
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
      final String handlerLabels = injector.getNamedInstance(Handlers.class);
      final Set<String> handlerLabelSet =
          new HashSet<>(Lists.newArrayList(handlerLabels.split(HANDLER_LABEL_SEPERATOR)));
      final String runtime = injector.getNamedInstance(Runtime.class);
      final int jobNum = injector.getNamedInstance(JobId.class);
      final String jobId = String.format("bridge.%d",
          jobNum < 0 ? System.currentTimeMillis() : jobNum);

      final Configuration runtimeConfig = getClientConfiguration(commandLineConf, runtime);
      final Configuration driverBridgeConfig = getDriverConfiguration(jobId, handlerLabelSet);
      final Configuration submittedConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(driverBridgeConfig, commandLineConf).build();

      DriverLauncher.getLauncher(runtimeConfig).run(submittedConfiguration);

      LOG.log(Level.INFO, "JavaBridge: Stop Client {0}", jobId);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "The handlers that should be configured. " + HANDLER_LABEL_DESCRIPTION,
      short_name = "handlers", default_value = "start")
  public static final class Handlers implements Name<String> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "The runtime to use: local, yarn, azbatch",
      short_name = "runtime", default_value = "local")
  public static final class Runtime implements Name<String> {
  }

  /**
   * Command line parameter = Numeric ID for the job.
   */
  @NamedParameter(doc = "Numeric ID for the job", short_name = "id", default_value = "-1")
  public static final class JobId implements Name<Integer> {
  }
}
