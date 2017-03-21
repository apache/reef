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

import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.client.JobStatusHandler;
import org.apache.reef.runtime.common.launch.ProfilingStopHandler;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.REEFVersion;
import org.apache.reef.wake.profiler.WakeProfiler;
import org.apache.reef.wake.time.Clock;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main entry point into any REEF process (Driver and Evaluator).
 * It is mostly reading from the command line to instantiate
 * the runtime clock and calling .run() on it.
 */
public final class REEFEnvironment implements Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(REEFEnvironment.class.getName());

  private static final String CLASS_NAME = REEFEnvironment.class.getCanonicalName();

  private static final Tang TANG = Tang.Factory.getTang();

  /** Main event loop of current REEF component (Driver or Evaluator). */
  private final Clock clock;

  /** Error handler that processes all uncaught REEF exceptions. */
  private final REEFErrorHandler errorHandler;

  private final JobStatusHandler jobStatusHandler;

  /**
   * Create a new REEF environment.
   * @param configurations REEF component (Driver or Evaluator) configuration.
   * If multiple configurations are provided, they will be merged before use.
   * Main part of the configuration is usually read from config file by REEFLauncher.
   * @throws InjectionException Thrown on configuration error.
   */
  @SuppressWarnings("checkstyle:illegalcatch") // Catch throwable to feed it to error handler
  public static REEFEnvironment fromConfiguration(final Configuration... configurations) throws InjectionException {

    final Configuration config = Configurations.merge(configurations);

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Configuration:\n--\n{0}\n--", Configurations.toString(config, true));
    }

    final Injector injector = TANG.newInjector(config);

    if (injector.getNamedInstance(WakeProfiler.ProfilingEnabled.class)) {
      final WakeProfiler profiler = new WakeProfiler();
      ProfilingStopHandler.setProfiler(profiler);
      injector.bindAspect(profiler);
    }

    injector.getInstance(REEFVersion.class).logVersion();

    final REEFErrorHandler errorHandler = injector.getInstance(REEFErrorHandler.class);
    final JobStatusHandler jobStatusHandler = injector.getInstance(JobStatusHandler.class);

    try {

      final Clock clock = injector.getInstance(Clock.class);
      return new REEFEnvironment(clock, errorHandler, jobStatusHandler);

    } catch (final Throwable ex) {
      LOG.log(Level.SEVERE, "Error while instantiating the clock", ex);
      try {
        errorHandler.onNext(ex);
      } catch (final Throwable exHandling) {
        LOG.log(Level.SEVERE, "Error while handling the exception " + ex, exHandling);
      }
      throw ex;
    }
  }

  /**
   * Use .fromConfiguration() method to create new REEF environment.
   * @param clock main event loop.
   * @param errorHandler error handler.
   * @param jobStatusHandler an object that receives notifications on job status changes
   * and can be queried for the last received job status.
   */
  private REEFEnvironment(
      final Clock clock, final REEFErrorHandler errorHandler, final JobStatusHandler jobStatusHandler) {

    this.clock = clock;
    this.errorHandler = errorHandler;
    this.jobStatusHandler = jobStatusHandler;
  }

  /**
   * Close and cleanup the environment.
   * Invoke .close() on all closeable members (clock and error handler).
   */
  @Override
  @SuppressWarnings("checkstyle:illegalcatch") // Catch throwable to feed it to error handler
  public void close() {

    LOG.entering(CLASS_NAME, "close");

    try {
      this.clock.close();
    } catch (final Throwable ex) {
      LOG.log(Level.SEVERE, "Error while closing the clock", ex);
      try {
        this.errorHandler.onNext(ex);
      } catch (final Throwable exHandling) {
        LOG.log(Level.SEVERE, "Error while handling the exception " + ex, exHandling);
      }
    } finally {
      try {
        this.errorHandler.close();
      } catch (final Throwable ex) {
        LOG.log(Level.SEVERE, "Error while closing the error handler", ex);
      }
    }

    LOG.exiting(CLASS_NAME, "close");
  }

  /**
   * Launch REEF component (Driver or Evaluator).
   * It is usually called from the static .run() method.
   * Check the status of the run via .getLastStatus() method.
   */
  @Override
  @SuppressWarnings("checkstyle:illegalcatch") // Catch throwable to feed it to error handler
  public void run() {

    LOG.log(Level.FINE, "REEF started with user name [{0}]", System.getProperty("user.name"));
    LOG.log(Level.FINE, "REEF started. Assertions are {0} in this process.",
            EnvironmentUtils.areAssertionsEnabled() ? "ENABLED" : "DISABLED");

    try {

      LOG.log(Level.FINEST, "Clock: start");
      this.clock.run();
      LOG.log(Level.FINEST, "Clock: exit normally: {0}", this.getLastStatus());

    } catch (final Throwable ex) {
      LOG.log(Level.SEVERE, "Clock: Error in main event loop", ex);
      this.errorHandler.onNext(ex);
    }
  }

  /**
   * Get the last known status of REEF job. Can return null if job has not started yet.
   * @return Status of the REEF job launched in this environment.
   */
  public ReefServiceProtos.JobStatusProto getLastStatus() {
    return this.jobStatusHandler.getLastStatus();
  }
}
