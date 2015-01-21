/**
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
package org.apache.reef.examples.retained_evalCLR;

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
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Retained Evaluators example - main class.
 */
public final class Launch {

  /**
   * Number of REEF worker threads in local mode.
   */
  private static final int NUM_LOCAL_THREADS = JobDriver.totalEvaluators;
  /**
   * Standard Java logger
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
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(Command.class);
    cl.registerShortNameOfClass(NumRuns.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(Command.class, injector.getNamedInstance(Command.class));
    cb.bindNamedParameter(NumRuns.class, String.valueOf(injector.getNamedInstance(NumRuns.class)));
    return cb.build();
  }

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param args Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   * @throws IOException        error reading the configuration.
   */
  private static Configuration getClientConfiguration(final String[] args)
      throws BindException, InjectionException, IOException {

    final Configuration commandLineConf = parseCommandLine(args);

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, JobClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, JobClient.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, JobClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, JobClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, JobClient.RuntimeErrorHandler.class)
        .build();

    // TODO: Remove the injector, have stuff injected.
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
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

  private static ConfigurationModule addAll(final ConfigurationModule conf, final OptionalParameter<String> param, final File folder) {
    ConfigurationModule result = conf;
    for (final File f : folder.listFiles()) {
      if (f.canRead() && f.exists() && f.isFile()) {
        result = result.set(param, f.getAbsolutePath());
      }
    }
    return result;
  }

  /**
   * Main method that starts the Retained Evaluators job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      final File dotNetFolder = new File(args[0]).getAbsoluteFile();
      String[] removedArgs = Arrays.copyOfRange(args, 1, args.length);

      final Configuration config = getClientConfiguration(removedArgs);
      LOG.log(Level.INFO, "Configuration:\n--\n{0}--",
          new AvroConfigurationSerializer().toString(config));
      final Injector injector = Tang.Factory.getTang().newInjector(config);
      final JobClient client = injector.getInstance(JobClient.class);
      client.submit(dotNetFolder);
      client.waitForCompletion();
      LOG.info("Done!");
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  /**
   * Command line parameter: a command to run. e.g. "echo Hello REEF"
   */
  @NamedParameter(doc = "The shell command", short_name = "cmd", default_value = "*INTERACTIVE*")
  public static final class Command implements Name<String> {
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
}
