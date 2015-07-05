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
package org.apache.reef.examples.data.output;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.data.output.*;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the output service demo app.
 */
@ClientSide
public final class OutputServiceREEF {
  private static final Logger LOG = Logger.getLogger(OutputServiceREEF.class.getName());

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    new CommandLine(cb)
        .registerShortNameOfClass(Local.class)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(OutputDir.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());
    final boolean isLocal = injector.getNamedInstance(Local.class);
    final String outputDir = injector.getNamedInstance(OutputDir.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;

    final Configuration driverConf = getDriverConf();
    final Configuration outputServiceConf = getOutputServiceConf(isLocal, outputDir);
    final Configuration submittedConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(driverConf, outputServiceConf)
        .build();
    final LauncherStatus state = DriverLauncher.getLauncher(getRuntimeConf(isLocal))
        .run(submittedConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * @param isLocal true for local runtime, or false for YARN runtime.
   * @return The runtime configuration
   */
  private static Configuration getRuntimeConf(final boolean isLocal) {
    final Configuration runtimeConf;
    if (isLocal) {
      LOG.log(Level.INFO, "Running the output service demo on the local runtime");
      runtimeConf = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 3)
          .build();
    } else {
      LOG.log(Level.INFO, "Running the output service demo on YARN");
      runtimeConf = YarnClientConfiguration.CONF.build();
    }
    return runtimeConf;
  }

  /**
   * @return The Driver configuration.
   */
  private static Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(OutputServiceDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "OutputServiceREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OutputServiceDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, OutputServiceDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, OutputServiceDriver.ActiveContextHandler.class)
        .build();

    return driverConf;
  }

  /**
   * @param isLocal true for local runtime, or false for YARN runtime.
   * @param outputDir path of the output directory.
   * @return The configuration to use OutputService
   */
  private static Configuration getOutputServiceConf(final boolean isLocal, final String outputDir) {
    final Configuration outputServiceConf;
    if (isLocal) {
      outputServiceConf = TaskOutputServiceBuilder.CONF
          .set(TaskOutputServiceBuilder.TASK_OUTPUT_STREAM_PROVIDER, TaskOutputStreamProviderLocal.class)
          .set(TaskOutputServiceBuilder.OUTPUT_PATH, getAbsolutePath(outputDir))
          .build();
    } else {
      outputServiceConf = TaskOutputServiceBuilder.CONF
          .set(TaskOutputServiceBuilder.TASK_OUTPUT_STREAM_PROVIDER, TaskOutputStreamProviderHDFS.class)
          .set(TaskOutputServiceBuilder.OUTPUT_PATH, outputDir)
          .build();
    }
    return outputServiceConf;
  }

  /**
   * transform the given relative path into the absolute path based on the current directory where a user runs the demo.
   * @param relativePath relative path
   * @return absolute path
   */
  private static String getAbsolutePath(final String relativePath) {
    final File outputFile = new File(relativePath);
    return outputFile.getAbsolutePath();
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Command line parameter = number of minutes before timeout.
   */
  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  /**
   * Command line parameter = path of the output directory.
   */
  @NamedParameter(doc = "Path of the output directory",
      short_name = "output")
  public static final class OutputDir implements Name<String> {
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private OutputServiceREEF() {
  }
}
