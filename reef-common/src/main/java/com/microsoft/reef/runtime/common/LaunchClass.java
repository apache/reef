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
package com.microsoft.reef.runtime.common;

import com.microsoft.reef.runtime.common.evaluator.PIDStoreStartHandler;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.wake.profiler.WakeProfiler;
import com.microsoft.wake.remote.RemoteConfiguration;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This encapsulates processes started by REEF.
 */
public final class LaunchClass implements AutoCloseable, Runnable {

  private static final Logger LOG = Logger.getLogger(LaunchClass.class.getName());
  private final RemoteManager remoteManager;
  private final String launchID;
  private final String errorHandlerID;
  private final String evaluatorConfigurationPath;
  private final boolean isProfilingEnabled;
  private final REEFErrorHandler errorHandler;
  private WakeProfiler profiler;

  @Inject
  LaunchClass(final RemoteManager remoteManager,
              final REEFUncaughtExceptionHandler uncaughtExceptionHandler,
              final REEFErrorHandler errorHandler,
              final @Parameter(Launcher.LaunchID.class) String launchID,
              final @Parameter(Launcher.ErrorHandlerRID.class) String errorHandlerID,
              final @Parameter(Launcher.EvaluatorConfigurationFilePath.class) String evaluatorConfigurationPath,
              final @Parameter(ProfilingEnabled.class) boolean enableProfiling) {
    this.remoteManager = remoteManager;
    this.launchID = launchID;
    this.errorHandlerID = errorHandlerID;
    this.evaluatorConfigurationPath = evaluatorConfigurationPath;
    this.isProfilingEnabled = enableProfiling;
    this.errorHandler = errorHandler;


    // Registering a default exception handler. It sends every exception to the upstream RemoteManager
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);


    if (isProfilingEnabled) {
      this.profiler = new WakeProfiler();
      ProfilingStopHandler.setProfiler(profiler); // TODO: This probably should be bound via Tang.
    }
  }

  /**
   * Loads the client and runtime configuration files from disk.
   */
  private Configuration getClockConfiguration() {
    final JavaConfigurationBuilder clockConfigurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    LOG.log(Level.FINE, "Loading configfile: " + this.evaluatorConfigurationPath);
    final File evaluatorConfigFile = new File(this.evaluatorConfigurationPath);
    try {

      if (!evaluatorConfigFile.exists()) {
        throw new IOException("File " + this.evaluatorConfigurationPath + " does not exist");
      }
      if (!evaluatorConfigFile.isFile()) {
        throw new IOException("File " + this.evaluatorConfigurationPath + " is not a file");
      }
      if (!evaluatorConfigFile.canRead()) {
        throw new IOException("File " + this.evaluatorConfigurationPath + " cannot be read");
      }

      ConfigurationFile.addConfiguration(clockConfigurationBuilder, evaluatorConfigFile);
      clockConfigurationBuilder.bindNamedParameter(Launcher.LaunchID.class, this.launchID);
      clockConfigurationBuilder.bindNamedParameter(Launcher.ErrorHandlerRID.class, this.errorHandlerID);
      clockConfigurationBuilder.bindSetEntry(Clock.StartHandler.class, PIDStoreStartHandler.class);
      clockConfigurationBuilder.bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class);
      clockConfigurationBuilder.bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER");
      if (isProfilingEnabled) {
        clockConfigurationBuilder.bindSetEntry(Clock.StopHandler.class, ProfilingStopHandler.class);
      }
    } catch (final Throwable throwable) {
      fail("Unable to read clock configuration", throwable);
    }
    return clockConfigurationBuilder.build();
  }

  /**
   * Instantiates the clock.
   *
   * @return a clock object.
   */
  private Clock getClock() {
    try {
      final Injector clockInjector = Tang.Factory.getTang().newInjector(this.getClockConfiguration());
      if (isProfilingEnabled) {
        clockInjector.bindAspect(profiler);
      }
      return clockInjector.getInstance(Clock.class);
    } catch (final Throwable ex) {
      fail("Unable to instantiate the clock", ex);
      throw new RuntimeException("Unable to instantiate the clock", ex);
    }
  }

  /**
   * Starts the Clock.
   * This blocks until the clock returns.
   */
  @Override
  public void run() {
    LOG.entering(this.getClass().getName(), "run", "Starting the clock");
    try {
      this.getClock().run();
    } catch (final Throwable t) {
      fail("Fatal exception while executing the clock", t);
    }
    LOG.exiting(this.getClass().getName(), "run", "Clock terminated");
  }

  /**
   * Closes the remote manager managed by this class.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    LOG.entering(this.getClass().getName(), "close");
    this.errorHandler.close(); // Also closes the remoteManager
    LOG.exiting(this.getClass().getName(), "close");
  }

  private void fail(final String message, final Throwable throwable) {
    this.errorHandler.onNext(new Exception(message, throwable));
  }

  @NamedParameter(doc = "If true, profiling will be enabled", short_name = "profiling", default_value = "false")
  public final static class ProfilingEnabled implements Name<Boolean> {
  }
}
