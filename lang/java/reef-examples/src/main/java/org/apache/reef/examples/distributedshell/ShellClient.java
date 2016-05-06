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
package org.apache.reef.examples.distributedshell;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.library.Command;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** The Client for REEF distributed shell example. */
public final class ShellClient {

  private static final Logger LOG = Logger.getLogger(ShellClient.class.getName());

  /** Number of milliseconds to wait for the job to complete. */
  private static final int JOB_TIMEOUT = 60000; // 1 min.

  private static final Tang TANG = Tang.Factory.getTang();

  private static final Configuration STATIC_DRIVER_CONFIG = DriverConfiguration.CONF
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "DistributedShell")
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ShellDriver.class))
      .set(DriverConfiguration.ON_DRIVER_STARTED, ShellDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ShellDriver.EvaluatorAllocatedHandler.class)
      .build();

  /**
   * Start the distributed shell job.
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException, IOException {

    final JavaConfigurationBuilder driverConfigBuilder = TANG.newConfigurationBuilder(STATIC_DRIVER_CONFIG);

    new CommandLine(driverConfigBuilder)
        .registerShortNameOfClass(Command.class)
        .registerShortNameOfClass(NumEvaluators.class)
        .registerShortNameOfClass(RuntimeName.class)
        .processCommandLine(args);

    final Configuration driverConfig = driverConfigBuilder.build();

    final Injector injector = TANG.newInjector(driverConfig);

    final int numEvaluators = injector.getNamedInstance(NumEvaluators.class);
    final String runtimeName = injector.getNamedInstance(RuntimeName.class);
    final String command = injector.getNamedInstance(Command.class);

    LOG.log(Level.INFO, "REEF distributed shell: {0} evaluators on {1} runtime :: {2}",
        new Object[] {numEvaluators, runtimeName, command});

    final Configuration runtimeConfig;

    switch (runtimeName) {
    case "local":
      runtimeConfig = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, numEvaluators)
          .build();
      break;
    case "yarn":
      runtimeConfig = YarnClientConfiguration.CONF.build();
      break;
    default:
      LOG.log(Level.SEVERE, "Unknown runtime: {0}", runtimeName);
      throw new IllegalArgumentException("Unknown runtime: " + runtimeName);
    }

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfig).run(driverConfig, JOB_TIMEOUT);

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /** This class should never be instantiated. */
  private ShellClient() { }
}
