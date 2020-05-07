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
package org.apache.reef.examples.scheduler.client;

import org.apache.commons.cli.ParseException;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.examples.scheduler.driver.SchedulerDriver;
import org.apache.reef.examples.scheduler.driver.http.SchedulerHttpHandler;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.webserver.HttpHandlerConfiguration;

import java.io.IOException;

/**
 * REEF TaskScheduler.
 */
public final class SchedulerREEF {
  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 3;

  /**
   * Command line parameter = true to reuse evaluators,.
   * or false to allocate/close for each iteration
   */
  @NamedParameter(doc = "Whether or not to reuse evaluators",
      short_name = "retain", default_value = "true")
  public static final class Retain implements Name<Boolean> {
  }

  /**
   * @return The http configuration to use reef-webserver
   */
  private static Configuration getHttpConf() {
    final Configuration httpHandlerConf = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, SchedulerHttpHandler.class)
        .build();
    return httpHandlerConf;
  }

  /**
   * @return The Driver configuration.
   */
  private static Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SchedulerDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TaskScheduler")
        .set(DriverConfiguration.ON_DRIVER_STARTED, SchedulerDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SchedulerDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SchedulerDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, SchedulerDriver.CompletedTaskHandler.class)
        .build();

    return driverConf;
  }

  /**
   * Run the Task scheduler. If '-retain true' option is passed via command line,
   * the scheduler reuses evaluators to submit new Tasks.
   * @param runtimeConf The runtime configuration (e.g. Local, YARN, etc)
   * @param args Command line arguments.
   * @throws InjectionException
   * @throws java.io.IOException
   */
  public static void runTaskScheduler(final Configuration runtimeConf, final String[] args)
      throws InjectionException, IOException, ParseException {
    final Tang tang = Tang.Factory.getTang();

    final Configuration commandLineConf = CommandLine.parseToConfiguration(args, Retain.class);

    // Merge the configurations to run Driver
    final Configuration driverConf = Configurations.merge(getDriverConf(), getHttpConf(), commandLineConf);

    final REEF reef = tang.newInjector(runtimeConf).getInstance(REEF.class);
    reef.submit(driverConf);
  }

  /**
   * Main program.
   * @param args
   * @throws InjectionException
   */
  public static void main(final String[] args) throws InjectionException, IOException, ParseException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
    runTaskScheduler(runtimeConfiguration, args);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private SchedulerREEF() {
  }
}
