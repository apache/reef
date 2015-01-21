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
package org.apache.reef.runtime.common.launch;

import org.apache.reef.runtime.common.evaluator.PIDStoreStartHandler;
import org.apache.reef.runtime.common.launch.parameters.ClockConfigurationPath;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.REEFVersion;
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
  private final ConfigurationSerializer configurationSerializer;
  private WakeProfiler profiler;

  @Inject
  LaunchClass(final RemoteManager remoteManager,
              final REEFUncaughtExceptionHandler uncaughtExceptionHandler,
              final REEFErrorHandler errorHandler,
              final @Parameter(LaunchID.class) String launchID,
              final @Parameter(ErrorHandlerRID.class) String errorHandlerID,
              final @Parameter(ClockConfigurationPath.class) String evaluatorConfigurationPath,
              final @Parameter(ProfilingEnabled.class) boolean enableProfiling,
              final ConfigurationSerializer configurationSerializer,
              final REEFVersion reefVersion) {
    reefVersion.logVersion();
    this.remoteManager = remoteManager;
    this.launchID = launchID;
    this.errorHandlerID = errorHandlerID;
    this.evaluatorConfigurationPath = evaluatorConfigurationPath;
    this.isProfilingEnabled = enableProfiling;
    this.errorHandler = errorHandler;
    this.configurationSerializer = configurationSerializer;


    // Registering a default exception handler. It sends every exception to the upstream RemoteManager
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);


    if (isProfilingEnabled) {
      this.profiler = new WakeProfiler();
      ProfilingStopHandler.setProfiler(profiler); // TODO: This probably should be bound via Tang.
    }
  }

  /**
   * Loads the client and resource manager configuration files from disk.
   */
  private Configuration getClockConfiguration() {
    return Configurations.merge(readConfigurationFromDisk(), getStaticClockConfiguration());
  }


  private Configuration readConfigurationFromDisk() {
    LOG.log(Level.FINEST, "Loading configuration file: {0}", this.evaluatorConfigurationPath);

    final File evaluatorConfigFile = new File(this.evaluatorConfigurationPath);

    if (!evaluatorConfigFile.exists()) {
      final String message = "The configuration file " + this.evaluatorConfigurationPath +
          "doesn't exist. This points to an issue in the job submission.";
      fail(message, new FileNotFoundException());
      throw new RuntimeException(message);
    } else if (!evaluatorConfigFile.canRead()) {
      final String message = "The configuration file " + this.evaluatorConfigurationPath + " exists, but can't be read";
      fail(message, new IOException());
      throw new RuntimeException(message);
    } else {
      try {
        return this.configurationSerializer.fromFile(evaluatorConfigFile);
      } catch (final IOException e) {
        final String message = "Unable to parse the configuration file " + this.evaluatorConfigurationPath;
        fail(message, e);
        throw new RuntimeException(message, e);
      }
    }
  }

  /**
   * @return the part of the clock configuration *not* read from disk.
   */
  private Configuration getStaticClockConfiguration() {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LaunchID.class, this.launchID)
        .bindNamedParameter(ErrorHandlerRID.class, this.errorHandlerID)
        .bindSetEntry(Clock.StartHandler.class, PIDStoreStartHandler.class)
        .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_LAUNCHER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class);
    if (this.isProfilingEnabled) {
      builder.bindSetEntry(Clock.StopHandler.class, ProfilingStopHandler.class);
    }
    return builder.build();
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
      clockInjector.bindVolatileInstance(RemoteManager.class, this.remoteManager);
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
