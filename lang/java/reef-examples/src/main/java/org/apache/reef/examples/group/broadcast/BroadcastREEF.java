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
package org.apache.reef.examples.group.broadcast;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.group.bgd.parameters.ModelDimensions;
import org.apache.reef.examples.group.broadcast.parameters.NumberOfReceivers;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for broadcast example.
 */
@ClientSide
public final class BroadcastREEF {

  private static final Logger LOG = Logger.getLogger(BroadcastREEF.class.getName());

  private static final String MAX_NUMBER_OF_EVALUATORS = "20";

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 2 * 60 * 1000;

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Input path.
   */
  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  private static boolean local;
  private static int dimensions;
  private static int numberOfReceivers;

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(ModelDimensions.class);
      cl.registerShortNameOfClass(NumberOfReceivers.class);
      cl.processCommandLine(aArgs);
    } catch (final IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration.
   */
  private static void storeCommandLineArgs(
      final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    local = injector.getNamedInstance(Local.class);
    dimensions = injector.getNamedInstance(ModelDimensions.class);
    numberOfReceivers = injector.getNamedInstance(NumberOfReceivers.class);
  }

  /**
   * @return (immutable) TANG Configuration object.
   */
  private static Configuration getRunTimeConfiguration() {
    final Configuration runtimeConfiguration;
    if (local) {
      LOG.log(Level.INFO, "Running Broadcast example using group API on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Broadcast example using group API on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfiguration;
  }

  public static LauncherStatus runBGDReef(
      final Configuration runtimeConfiguration) throws InjectionException {

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getAllClasspathJars())
        .set(DriverConfiguration.ON_DRIVER_STARTED, BroadcastDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, BroadcastDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BroadcastDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, BroadcastDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, BroadcastDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "BroadcastDriver")
        .build();

    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, driverConfiguration)
        .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(NumberOfReceivers.class, Integer.toString(numberOfReceivers))
        .build();

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "Merged driver configuration:\n{0}",
          Configurations.toString(mergedDriverConfiguration));
    }

    return DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, JOB_TIMEOUT);
  }

  public static void main(final String[] args) throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final Configuration runtimeConfiguration = getRunTimeConfiguration();
    final LauncherStatus state = runBGDReef(runtimeConfiguration);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private BroadcastREEF() {
  }
}
