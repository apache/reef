/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.examples.ds;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.utils.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationFile;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Shell example - main class.
 */
public final class DSClient {

  /**
   * This class should not be instantiated.
   */
  private DSClient() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Number of REEF worker threads in local mode.
   */
  private static final int NUM_LOCAL_THREADS = 2;

  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(DSClient.class.getName());

  /**
   * Command line parameter - a command to run. e.g. -cmd "echo Hello REEF"
   */
  @NamedParameter(doc = "The shell command", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  static final String EMPTY_FILES = "EMPTY_FILES";
  /**
   * Command line parameter - additional files to submitActivity to the Evaluators.
   */
  @NamedParameter(doc = "Additional files to submitActivity to the evaluators.",
      short_name = "files", default_value = EMPTY_FILES)
  public static final class Files implements Name<String> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * @param aArgs command line arguments, as passed to main()
   * @return Configuration object.
   */
  @SuppressWarnings("unchecked")
  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Command.class);
      cl.registerShortNameOfClass(Files.class);
      cl.registerShortNameOfClass(Local.class);
      cl.processCommandLine(aArgs);
    } catch (final BindException | IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * @param commandLineConf Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration injector fails.
   * @throws InjectionException if the Local.class parameter is not injected.
   */
  private static Configuration getClientConfiguration(final Configuration commandLineConf)
      throws BindException, InjectionException {

    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

    final Configuration runtimeConfiguration;
    if (commandLineInjector.getNamedInstance(Local.class)) {
      LOG.log(Level.INFO, "Running DS on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running DS on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF
          .set(YarnClientConfiguration.REEF_JAR_FILE, EnvironmentUtils.getClassLocationFile(REEF.class))
          .build();
    }

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_MESSAGE, DistributedShell.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, DistributedShell.CompletedJobHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(runtimeConfiguration, clientConfiguration,
            cloneCommandLineConfiguration(commandLineConf))
        .build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration
   */
  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    cb.bindNamedParameter(Local.class, injector.getNamedInstance(Local.class).toString());
    cb.bindNamedParameter(Command.class, injector.getNamedInstance(Command.class));
    cb.bindNamedParameter(Files.class, injector.getNamedInstance(Files.class));
    return cb.build();
  }

  /**
   * Launch REEF job and wait for its completion.
   *
   * @param aConfig TANG configuration.
   * @return Concatenated output from all distributed shells.
   * @throws BindException        configuration error
   * @throws InjectionException   configuration error
   */
  public static String runDistributedShell(final Configuration aConfig)
      throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(aConfig);
    final DistributedShell shell = injector.getInstance(DistributedShell.class);
    final String cmd = injector.getNamedInstance(Command.class);
    shell.submit(cmd);
    return shell.getResult();
  }

  /**
   * Main method that runs the Distributed Shell.
   *
   * @param aArgs command line parameters.
   */
  public static void main(final String[] aArgs) {
    try {
      final Configuration commandLineConf = parseCommandLine(aArgs);

      final Configuration config = getClientConfiguration(commandLineConf);
      LOG.log(Level.INFO, "DS Configuration:\n--\n{0}--",
          ConfigurationFile.toConfigurationString(config));
      final String dsResult = runDistributedShell(config);
      LOG.log(Level.INFO, "Distributed shell result: {0}\n", dsResult);
      System.out.println(dsResult);
    } catch (final BindException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Unable to launch distributed shell", ex);
    }
  }
}
