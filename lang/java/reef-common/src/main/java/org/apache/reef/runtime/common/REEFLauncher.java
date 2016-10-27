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
package org.apache.reef.runtime.common;

import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.common.launch.REEFUncaughtExceptionHandler;
import org.apache.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;
import org.apache.reef.util.logging.LoggingSetup;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main entry point into any REEF process (Driver and Evaluator).
 * It is mostly reading from the command line to instantiate
 * the runtime clock and calling .run() on it.
 */
public final class REEFLauncher {

  private static final Logger LOG = Logger.getLogger(REEFLauncher.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  private static final Configuration LAUNCHER_STATIC_CONFIG =
      TANG.newConfigurationBuilder()
          .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER")
          .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
          .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          .bindSetEntry(Clock.RuntimeStartHandler.class, PIDStoreStartHandler.class)
          .build();

  static {
    LoggingSetup.setupCommonsLogging();
  }

  /**
   * Main configuration object of the REEF component we are launching here.
   * REEFEnvironment uses that configuration to instantiate the Clock object,
   * and then call .run() on it.
   */
  private final Configuration envConfig;

  /**
   * REEFLauncher is instantiated in the main() method below using
   * Tang configuration file provided as a command line argument.
   * @param configurationPath Path to the serialized Tang configuration file.
   * (The file must be in the local file system).
   * @param configurationSerializer Serializer used to read the configuration file.
   * We currently use Avro to serialize Tang configs.
   */
  @Inject
  private REEFLauncher(
      @Parameter(ClockConfigurationPath.class) final String configurationPath,
      final ConfigurationSerializer configurationSerializer) {

    this.envConfig = Configurations.merge(LAUNCHER_STATIC_CONFIG,
        readConfigurationFromDisk(configurationPath, configurationSerializer));
  }

  /**
   * Instantiate REEF Launcher. This method is called from REEFLauncher.main().
   * @param clockConfigPath Path to the local file that contains serialized configuration
   * of a REEF component to launch (can be either Driver or Evaluator).
   * @return An instance of the configured REEFLauncher object.
   */
  private static REEFLauncher getREEFLauncher(final String clockConfigPath) {

    try {

      final Configuration clockArgConfig = TANG.newConfigurationBuilder()
          .bindNamedParameter(ClockConfigurationPath.class, clockConfigPath)
          .build();

      return TANG.newInjector(clockArgConfig).getInstance(REEFLauncher.class);

    } catch (final BindException ex) {
      throw fatal("Error in parsing the command line", ex);
    } catch (final InjectionException ex) {
      throw fatal("Unable to instantiate REEFLauncher.", ex);
    }
  }

  /**
   * Read configuration from a given file and deserialize it
   * into Tang configuration object that can be used for injection.
   * Configuration is currently serialized using Avro.
   * This method also prints full deserialized configuration into log.
   * @param configPath Path to the local file that contains serialized configuration
   * of a REEF component to launch (can be either Driver or Evaluator).
   * @param serializer An object to deserialize the configuration file.
   * @return Tang configuration read and deserialized from a given file.
   */
  private static Configuration readConfigurationFromDisk(
          final String configPath, final ConfigurationSerializer serializer) {

    LOG.log(Level.FINER, "Loading configuration file: {0}", configPath);

    final File evaluatorConfigFile = new File(configPath);

    if (!evaluatorConfigFile.exists()) {
      throw fatal(
          "Configuration file " + configPath + " does not exist. Can be an issue in job submission.",
          new FileNotFoundException(configPath));
    }

    if (!evaluatorConfigFile.canRead()) {
      throw fatal(
          "Configuration file " + configPath + " exists, but can't be read.",
          new IOException(configPath));
    }

    try {

      final Configuration config = serializer.fromFile(evaluatorConfigFile);
      LOG.log(Level.FINEST, "Configuration file loaded: {0}", configPath);

      return config;

    } catch (final IOException e) {
      throw fatal("Unable to parse the configuration file: " + configPath, e);
    }
  }

  /**
   * Launches a REEF client process (Driver or Evaluator).
   * @param args Command-line arguments.
   * Must be a single element containing local path to the configuration file.
   */
  @SuppressWarnings("checkstyle:illegalcatch")
  public static void main(final String[] args) {

    LOG.log(Level.INFO, "Entering REEFLauncher.main().");

    LOG.log(Level.FINE, "REEFLauncher started with user name [{0}]", System.getProperty("user.name"));
    LOG.log(Level.FINE, "REEFLauncher started. Assertions are {0} in this process.",
            EnvironmentUtils.areAssertionsEnabled() ? "ENABLED" : "DISABLED");

    if (args.length != 1) {
      final String message = "REEFLauncher have one and only one argument to specify the runtime clock " +
          "configuration path";

      throw fatal(message, new IllegalArgumentException(message));
    }

    final REEFLauncher launcher = getREEFLauncher(args[0]);

    Thread.setDefaultUncaughtExceptionHandler(new REEFUncaughtExceptionHandler(launcher.envConfig));

    try (final REEFEnvironment reef = REEFEnvironment.fromConfiguration(launcher.envConfig)) {
      reef.run();
    } catch (final Throwable ex) {
      throw fatal("Unable to configure and start REEFEnvironment.", ex);
    }

    LOG.log(Level.INFO, "Exiting REEFLauncher.main()");

    System.exit(0);

    ThreadLogger.logThreads(LOG, Level.FINEST, "Threads running after System.exit():");
  }

  /**
   * Wrap an exception into RuntimeException with a given message,
   * and write the same message and exception to the log.
   * @param msg an error message to log and pass into the RuntimeException.
   * @param t A Throwable exception to log and wrap.
   * @return a new Runtime exception wrapping a Throwable.
   */
  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }
}
