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
import org.apache.reef.runtime.common.launch.ProfilingStopHandler;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.common.launch.REEFUncaughtExceptionHandler;
import org.apache.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.REEFVersion;
import org.apache.reef.util.ThreadLogger;
import org.apache.reef.util.logging.LoggingSetup;
import org.apache.reef.wake.profiler.WakeProfiler;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main entrance point into any REEF process. It is mostly reading from the command line to instantiate
 * the runtime clock and calling .run() on it.
 */
public final class REEFLauncher {

  /**
   * Parameter which enables profiling.
   */
  @NamedParameter(doc = "If true, profiling will be enabled", short_name = "profiling", default_value = "false")
  public static final class ProfilingEnabled implements Name<Boolean> {
  }

  private static final Logger LOG = Logger.getLogger(REEFLauncher.class.getName());

  private static final Configuration LAUNCHER_STATIC_CONFIG = Tang.Factory.getTang().newConfigurationBuilder()
          .bindSetEntry(Clock.StartHandler.class, PIDStoreStartHandler.class)
          .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
          .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER")
          .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          .build();

  static {
    LoggingSetup.setupCommonsLogging();
  }

  private final String configurationPath;
  private final boolean isProfilingEnabled;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFVersion reefVersion;
  private final Configuration clockConfig;

  @Inject
  private REEFLauncher(@Parameter(ClockConfigurationPath.class) final String configurationPath,
               @Parameter(ProfilingEnabled.class) final boolean enableProfiling,
               final ConfigurationSerializer configurationSerializer,
               final REEFVersion reefVersion) {
    this.configurationPath = configurationPath;
    this.configurationSerializer = configurationSerializer;
    this.isProfilingEnabled = enableProfiling;
    this.reefVersion = reefVersion;
    this.clockConfig = Configurations.merge(
            readConfigurationFromDisk(this.configurationPath, this.configurationSerializer),
            LAUNCHER_STATIC_CONFIG);
  }

  private static REEFLauncher getREEFLauncher(final String clockConfigPath) {
    final Injector injector;
    try {
      final Configuration clockArgConfig = Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(ClockConfigurationPath.class, clockConfigPath).build();
      injector = Tang.Factory.getTang().newInjector(clockArgConfig);
    } catch (final BindException e) {
      throw fatal("Error in parsing the command line", e);
    }

    try {
      return injector.getInstance(REEFLauncher.class);
    } catch (final InjectionException e) {
      throw fatal("Unable to run REEFLauncher.", e);
    }
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private static RuntimeException fatal(final REEFErrorHandler errorHandler, final String msg, final Throwable t) {
    errorHandler.onNext(t);
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }

  private static Configuration readConfigurationFromDisk(
          final String configPath, final ConfigurationSerializer serializer) {
    LOG.log(Level.FINEST, "Loading configuration file: {0}", configPath);

    final File evaluatorConfigFile = new File(configPath);

    if (!evaluatorConfigFile.exists()) {
      final String message = "The configuration file " + configPath +
          "doesn't exist. This points to an issue in the job submission.";
      throw fatal(message, new FileNotFoundException());
    } else if (!evaluatorConfigFile.canRead()) {
      final String message = "The configuration file " + configPath +
          " exists, but can't be read";
      throw fatal(message, new IOException());
    } else {
      try {
        return serializer.fromFile(evaluatorConfigFile);
      } catch (final IOException e) {
        final String message = "Unable to parse the configuration file " + configPath;
        throw fatal(message, e);
      }
    }
  }

  /**
   * Launches a REEF client process (Driver or Evaluator).
   *
   * @param args command-line args
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

    Thread.setDefaultUncaughtExceptionHandler(new REEFUncaughtExceptionHandler(launcher.clockConfig));
    launcher.logVersion();

    try (final Clock clock = launcher.getClockFromConfig()) {
      LOG.log(Level.FINE, "Clock starting");
      clock.run();
      LOG.log(Level.FINE, "Clock exiting");
    } catch (final Throwable ex) {
      try (final REEFErrorHandler errorHandler = launcher.getErrorHandlerFromConfig()) {
        throw fatal(errorHandler, "Unable to instantiate the clock", ex);
      } catch (final InjectionException e) {
        throw fatal("Unable to instantiate the clock and the ErrorHandler", e);
      }
    }

    LOG.log(Level.INFO, "Exiting REEFLauncher.main()");
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, ThreadLogger.getFormattedThreadList("Threads running after REEFLauncher.close():"));
    }
    System.exit(0);
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, ThreadLogger.getFormattedThreadList("Threads running after System.exit():"));
    }
  }

  private void logVersion() {
    this.reefVersion.logVersion();
  }

  /**
   * A new REEFErrorHandler is instantiated instead of lazy instantiation and saving the instantiated
   * handler as a field since the ErrorHandler is closeable.
   * @return A new REEFErrorHandler from clock config
   * @throws InjectionException
   */
  private REEFErrorHandler getErrorHandlerFromConfig() throws InjectionException {
    return Tang.Factory.getTang().newInjector(this.clockConfig).getInstance(REEFErrorHandler.class);
  }

  /**
   * A new Clock is instantiated instead of lazy instantiation and saving the instantiated
   * handler as a field since the Clock is closeable.
   * @return A new Clock from clock config
   * @throws InjectionException
   */
  private Clock getClockFromConfig() throws InjectionException {
    final Injector clockInjector = Tang.Factory.getTang().newInjector(this.clockConfig);
    if (this.isProfilingEnabled) {
      final WakeProfiler profiler = new WakeProfiler();
      ProfilingStopHandler.setProfiler(profiler);
      clockInjector.bindAspect(profiler);
    }

    return clockInjector.getInstance(Clock.class);
  }
}
