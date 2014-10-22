/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.pool;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.CommandLine;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pool of Evaluators example - main class.
 */
public final class Launch {

  /**
   * This class should not be instantiated.
   */
  private Launch() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Number of REEF worker threads in local mode.
   */
  private static final int NUM_LOCAL_THREADS = 4;

  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * Command line parameter: number of Evaluators to request.
   */
  @NamedParameter(doc = "Number of evaluators to request", short_name = "evaluators")
  public static final class NumEvaluators implements Name<Integer> {
  }

  /**
   * Command line parameter: number of Tasks to run.
   */
  @NamedParameter(doc = "Number of tasks to run", short_name = "tasks")
  public static final class NumTasks implements Name<Integer> {
  }

  /**
   * Command line parameter: number of experiments to run.
   */
  @NamedParameter(doc = "Number of seconds to sleep in each task", short_name = "delay")
  public static final class Delay implements Name<Integer> {
  }

  /**
   * Command line parameter = true to submit task and context in one request.
   */
  @NamedParameter(doc = "Submit task and context together",
      short_name = "piggyback", default_value = "true")
  public static final class Piggyback implements Name<Boolean> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Command line parameter = Numeric ID for the job.
   */
  @NamedParameter(doc = "Numeric ID for the job", short_name = "id", default_value = "-1")
  public static final class JobId implements Name<Integer> {
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
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(Piggyback.class);
    cl.registerShortNameOfClass(NumEvaluators.class);
    cl.registerShortNameOfClass(NumTasks.class);
    cl.registerShortNameOfClass(Delay.class);
    cl.registerShortNameOfClass(JobId.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(Piggyback.class, String.valueOf(injector.getNamedInstance(Piggyback.class)));
    cb.bindNamedParameter(NumEvaluators.class, String.valueOf(injector.getNamedInstance(NumEvaluators.class)));
    cb.bindNamedParameter(NumTasks.class, String.valueOf(injector.getNamedInstance(NumTasks.class)));
    cb.bindNamedParameter(Delay.class, String.valueOf(injector.getNamedInstance(Delay.class)));
    return cb.build();
  }

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param commandLineConf Parsed command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   */
  private static Configuration getClientConfiguration(
      final Configuration commandLineConf, final boolean isLocal)
      throws BindException, InjectionException {
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.FINE, "Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.FINE, "Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return Tang.Factory.getTang().newConfigurationBuilder(
        runtimeConfiguration, cloneCommandLineConfiguration(commandLineConf))
        .build();
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {

    try {

      final Configuration commandLineConf = parseCommandLine(args);
      final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);

      final boolean isLocal = injector.getNamedInstance(Local.class);
      final int numEvaluators = injector.getNamedInstance(NumEvaluators.class);
      final int numTasks = injector.getNamedInstance(NumTasks.class);
      final int delay = injector.getNamedInstance(Delay.class);
      final int jobNum = injector.getNamedInstance(JobId.class);

      final String jobId = String.format("pool.e_%d.a_%d.d_%d.%d",
          numEvaluators, numTasks, delay, jobNum < 0 ? System.currentTimeMillis() : jobNum);

      // Timeout: delay + 6 extra seconds per Task per Evaluator + 2 minutes to allocate each Evaluator:
      final int timeout = numTasks * (delay + 6) * 1000 / numEvaluators + numEvaluators * 120000;

      final Configuration runtimeConfig = getClientConfiguration(commandLineConf, isLocal);
      LOG.log(Level.INFO, "TIME: Start Client {0} with timeout {1} sec. Configuration:\n--\n{2}--",
          new Object[]{jobId, timeout / 1000, new AvroConfigurationSerializer().toString(runtimeConfig)});

      final Configuration driverConfig = DriverConfiguration.CONF
          .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(JobDriver.class))
          .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId)
          .set(DriverConfiguration.ON_DRIVER_STARTED, JobDriver.StartHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STOP, JobDriver.StopHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, JobDriver.AllocatedEvaluatorHandler.class)
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, JobDriver.ActiveContextHandler.class)
          .set(DriverConfiguration.ON_TASK_RUNNING, JobDriver.RunningTaskHandler.class)
          .set(DriverConfiguration.ON_TASK_COMPLETED, JobDriver.CompletedTaskHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, JobDriver.CompletedEvaluatorHandler.class)
          .build();

      final Configuration submittedConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(driverConfig, commandLineConf).build();
      DriverLauncher.getLauncher(runtimeConfig)
          .run(submittedConfiguration, timeout);

      LOG.log(Level.INFO, "TIME: Stop Client {0}", jobId);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
