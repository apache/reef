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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.bridge.driver.client.grpc.DriverClientGrpcConfiguration;
import org.apache.reef.bridge.driver.client.grpc.parameters.DriverServicePort;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.common.launch.REEFUncaughtExceptionHandler;
import org.apache.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
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
 * Driver client launcher.
 */
@Unstable
public final class JavaDriverClientLauncher {

  private static final Logger LOG = Logger.getLogger(REEFLauncher.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  private static final Configuration LAUNCHER_STATIC_CONFIG =
      TANG.newConfigurationBuilder()
          .bindNamedParameter(RemoteConfiguration.ManagerName.class, "DRIVER_CLIENT_LAUNCHER")
          .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
          .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          .bindSetEntry(Clock.RuntimeStartHandler.class, PIDStoreStartHandler.class)
          .build();

  static {
    LoggingSetup.setupCommonsLogging();
  }

  /**
   * Main configuration object of the REEF component we are launching here.
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
  private JavaDriverClientLauncher(
      @Parameter(DriverServicePort.class) final Integer driverServicePort,
      @Parameter(ClockConfigurationPath.class) final String configurationPath,
      final ConfigurationSerializer configurationSerializer) {

    this.envConfig = Configurations.merge(
        LAUNCHER_STATIC_CONFIG,
        DriverClientGrpcConfiguration.CONF
            .set(DriverClientGrpcConfiguration.DRIVER_SERVICE_PORT, driverServicePort)
            .build(),
        readConfigurationFromDisk(configurationPath, configurationSerializer));
  }

  /**
   * Instantiate REEF DriverServiceLauncher. This method is called from REEFLauncher.main().
   * @param clockConfigPath Path to the local file that contains serialized configuration
   *                        for the driver client.
   * @return An instance of the configured REEFLauncher object.
   */
  private static JavaDriverClientLauncher getLauncher(final String clockConfigPath, final int driverServicePort) {
    try {
      final Configuration clockArgConfig = Configurations.merge(
          LAUNCHER_STATIC_CONFIG,
          DriverClientGrpcConfiguration.CONF
              .set(DriverClientGrpcConfiguration.DRIVER_SERVICE_PORT, driverServicePort)
              .build(),
          TANG.newConfigurationBuilder()
              .bindNamedParameter(ClockConfigurationPath.class, clockConfigPath)
              .build());

      return TANG.newInjector(clockArgConfig).getInstance(JavaDriverClientLauncher.class);
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
      LOG.log(Level.FINEST, "The configuration file loaded: {0}", configPath);

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

    LOG.log(Level.INFO, "Entering JavaDriverClientLauncher.main().");

    LOG.log(Level.FINE, "JavaDriverClientLauncher started with user name [{0}]", System.getProperty("user.name"));
    LOG.log(Level.FINE, "JavaDriverClientLauncher started. Assertions are {0} in this process.",
        EnvironmentUtils.areAssertionsEnabled() ? "ENABLED" : "DISABLED");

    if (args.length != 2) {
      final String message = "JavaDriverClientLauncher have two and only two arguments to specify the runtime clock " +
          "configuration path and driver service port";

      throw fatal(message, new IllegalArgumentException(message));
    }

    final JavaDriverClientLauncher launcher = getLauncher(args[0], Integer.parseInt(args[1]));

    Thread.setDefaultUncaughtExceptionHandler(new REEFUncaughtExceptionHandler(launcher.envConfig));
    final Injector injector = TANG.newInjector(launcher.envConfig);
    try {
      final DriverServiceClient driverServiceClient = injector.getInstance(DriverServiceClient.class);
      try (Clock reef = injector.getInstance(Clock.class)) {
        reef.run();
      } catch (final InjectionException ex) {
        LOG.log(Level.SEVERE, "Unable to configure driver client.");
        driverServiceClient.onInitializationException(ex.getCause() != null ? ex.getCause() : ex);
      } catch (final Throwable t) {
        if (t.getCause() != null && t.getCause() instanceof InjectionException) {
          LOG.log(Level.SEVERE, "Unable to configure driver client.");
          final InjectionException ex = (InjectionException) t.getCause();
          driverServiceClient.onInitializationException(ex.getCause() != null ? ex.getCause() : ex);
        } else {
          throw fatal("Unable run clock.", t);
        }
      }
    } catch (final InjectionException e) {
      throw fatal("Unable initialize driver service client.", e);
    }

    ThreadLogger.logThreads(LOG, Level.FINEST, "Threads running after Clock.close():");

    LOG.log(Level.INFO, "Exiting REEFLauncher.main()");

    System.exit(0); // TODO[REEF-1715]: Should be able to exit cleanly at the end of main()
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
