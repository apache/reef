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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
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
 * Suspend/Resume example - main class.
 */
public final class Launch {

  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Command line parameter: number of iterations to run.
   */
  @NamedParameter(doc = "Number of iterations to run", short_name = "cycles", default_value = "20")
  public static final class NumCycles implements Name<Integer> {
  }

  /**
   * Command line parameter: delay in seconds for each cycle.
   */
  @NamedParameter(doc = "Delay in seconds between the cycles", short_name = "delay", default_value = "1")
  public static final class Delay implements Name<Integer> {
  }

  /**
   * Number of REEF worker threads in local mode.
   */
  private static final int NUM_LOCAL_THREADS = 4;

  /**
   * This class should not be instantiated.
   */
  private Launch() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * @param args command line arguments, as passed to main()
   * @return Configuration object.
   */
  private static Configuration parseCommandLine(final String[] args)
      throws IOException, BindException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(NumCycles.class);
    cl.registerShortNameOfClass(Delay.class);
    cl.registerShortNameOfClass(SuspendClientControl.Port.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(NumCycles.class, String.valueOf(injector.getNamedInstance(NumCycles.class)));
    cb.bindNamedParameter(Delay.class, String.valueOf(injector.getNamedInstance(Delay.class)));
    cb.bindNamedParameter(SuspendClientControl.Port.class,
        String.valueOf(injector.getNamedInstance(SuspendClientControl.Port.class)));
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
        .set(ClientConfiguration.ON_JOB_RUNNING, SuspendClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, SuspendClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, SuspendClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, SuspendClient.RuntimeErrorHandler.class)
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

  /**
   * Main method that runs the example.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      final Configuration config = getClientConfiguration(args);

      LOG.log(Level.INFO, "Configuration:\n--\n{0}--",
          new AvroConfigurationSerializer().toString(config));

      final Injector injector = Tang.Factory.getTang().newInjector(config);
      final SuspendClient client = injector.getInstance(SuspendClient.class);

      client.submit();
      client.waitForCompletion();
      LOG.info("Done!");

    } catch (final BindException | IOException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Cannot launch: configuration error", ex);
    } catch (final Exception ex) {
      LOG.log(Level.SEVERE, "Cleanup error", ex);
    }
  }
}
