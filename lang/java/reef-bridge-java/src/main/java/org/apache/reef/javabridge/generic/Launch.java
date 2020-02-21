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
package org.apache.reef.javabridge.generic;

import org.apache.reef.client.ClientConfiguration;
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
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clr Bridge example - main class.
 */
public final class Launch {

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 10;
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
   * @throws org.apache.reef.tang.exceptions.BindException configuration error.
   * @throws java.io.IOException                           error reading the configuration.
   */
  private static Configuration parseCommandLine(final String[] args)
      throws BindException, IOException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(NumRuns.class);
    cl.registerShortNameOfClass(WaitTimeForDriver.class);
    cl.registerShortNameOfClass(DriverMemoryInMb.class);
    cl.registerShortNameOfClass(DriverIdentifier.class);
    cl.registerShortNameOfClass(DriverJobSubmissionDirectory.class);
    cl.registerShortNameOfClass(Submit.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(NumRuns.class, String.valueOf(injector.getNamedInstance(NumRuns.class)));
    return cb.build();
  }

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param args Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws org.apache.reef.tang.exceptions.BindException      if configuration commandLineInjector fails.
   * @throws org.apache.reef.tang.exceptions.InjectionException if configuration commandLineInjector fails.
   * @throws java.io.IOException                                error reading the configuration.
   */
  private static Configuration getClientConfiguration(final String[] args)
      throws BindException, InjectionException, IOException {

    try (LoggingScope ls = LoggingScopeFactory.getNewLoggingScope(Level.INFO, "Launch::getClientConfiguration")) {
      final Configuration commandLineConf = parseCommandLine(args);

      final Configuration clientConfiguration = ClientConfiguration.CONF
          .set(ClientConfiguration.ON_JOB_COMPLETED, JobClient.CompletedJobHandler.class)
          .set(ClientConfiguration.ON_JOB_FAILED, JobClient.FailedJobHandler.class)
          .set(ClientConfiguration.ON_RUNTIME_ERROR, JobClient.RuntimeErrorHandler.class)
          .build();

      final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
      final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
      final Configuration runtimeConfiguration;
      if (isLocal) {
        LOG.log(Level.INFO, "Running on the local runtime");
        runtimeConfiguration = LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
            .build();
      } else {
        LOG.log(Level.INFO, "Running on YARN");
        runtimeConfiguration = YarnClientConfiguration.CONF.build();
      }

      return Tang.Factory.getTang()
          .newConfigurationBuilder(runtimeConfiguration, clientConfiguration,
              cloneCommandLineConfiguration(commandLineConf))
          .build();
    }
  }

  /**
   * Main method that starts the CLR Bridge from Java.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    LOG.log(Level.INFO, "Entering Launch at :::" + new Date());
    try {
      if (args == null || args.length == 0) {
        throw new IllegalArgumentException("No arguments provided, at least a clrFolder should be supplied.");
      }
      final File dotNetFolder = new File(args[0]).getAbsoluteFile();
      final String[] removedArgs = Arrays.copyOfRange(args, 1, args.length);

      final Configuration config = getClientConfiguration(removedArgs);
      final Injector commandLineInjector = Tang.Factory.getTang().newInjector(parseCommandLine(removedArgs));
      final int waitTime = commandLineInjector.getNamedInstance(WaitTimeForDriver.class);
      final int driverMemory = commandLineInjector.getNamedInstance(DriverMemoryInMb.class);
      final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
      final String driverIdentifier = commandLineInjector.getNamedInstance(DriverIdentifier.class);
      final String jobSubmissionDirectory = commandLineInjector.getNamedInstance(DriverJobSubmissionDirectory.class);
      final boolean submit = commandLineInjector.getNamedInstance(Submit.class);
      final Injector injector = Tang.Factory.getTang().newInjector(config);
      final JobClient client = injector.getInstance(JobClient.class);
      client.setDriverInfo(driverIdentifier, driverMemory, jobSubmissionDirectory);

      if (submit) {
        client.submit(dotNetFolder, true, isLocal, null);
        client.waitForCompletion(waitTime);
      } else {
        client.submit(dotNetFolder, false, isLocal, config);
        client.waitForCompletion(0);
      }


      LOG.info("Done!");
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  /**
   * Command line parameter: number of experiments to run.
   */
  @NamedParameter(doc = "Number of times to run the command",
      short_name = "num_runs", default_value = "1")
  public static final class NumRuns implements Name<Integer> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Command line parameter, number of seconds  to wait till driver finishes ,
   * = -1 : waits forever
   * = 0: exit immediately without wait for driver.
   */
  @NamedParameter(doc = "Whether or not to wait for driver to finish",
      short_name = "wait_time", default_value = "-1")
  public static final class WaitTimeForDriver implements Name<Integer> {
  }

  /**
   * Command line parameter, driver memory, in MB.
   */
  @NamedParameter(doc = "memory allocated to driver JVM",
      short_name = "driver_memory", default_value = "512")
  public static final class DriverMemoryInMb implements Name<Integer> {
  }

  /**
   * Command line parameter, driver identifier.
   */
  @NamedParameter(doc = "driver identifier for clr bridge",
      short_name = "driver_id", default_value = "ReefClrBridge")
  public static final class DriverIdentifier implements Name<String> {
  }

  /**
   * Command line parameter = true to submit the job with driver config, or false to write config to current directory.
   */
  @NamedParameter(doc = "Whether or not to submit the reef job after driver config is constructed",
      short_name = "submit", default_value = "true")
  public static final class Submit implements Name<Boolean> {
  }

  /**
   * Command line parameter, job submission directory, if set, user should guarantee its uniqueness.
   */
  @NamedParameter(doc = "driver job submission directory",
      short_name = "submission_directory", default_value = "empty")
  public static final class DriverJobSubmissionDirectory implements Name<String> {
  }
}
