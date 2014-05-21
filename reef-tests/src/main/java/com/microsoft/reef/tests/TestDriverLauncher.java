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
package com.microsoft.reef.tests;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.client.*;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A launcher for REEF Drivers. It behaves exactly like the original DriverLauncher,
 * but does not dump the exception stack trace to the log. (it writes only a short INFO message).
 * It is used in TestFailDriver etc. unit tests.
 */
@Public
@Provided
@ClientSide
@Unit
public final class TestDriverLauncher {

  private static final Logger LOG = Logger.getLogger(TestDriverLauncher.class.getName());

  private final DriverLauncher launcher;

  @Inject
  private TestDriverLauncher(final DriverLauncher launcher) {
    this.launcher = launcher;
  }

  /**
   * Handler an error in the job driver.
   */
  protected final class SilentRuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.INFO, "Received a runtime error: {0}", error);
      launcher.setStatusAndNotify(LauncherStatus.FAILED(error.getReason()));
    }
  }

  /**
   * Job driver notifies us that the job had failed.
   */
  protected final class SilentFailedTestJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      final Optional<Throwable> ex = job.getReason();
      LOG.log(Level.INFO, "Received an error for job {0}: {1}", new Object[]{job.getId(), ex});
      launcher.setStatusAndNotify(LauncherStatus.FAILED(ex));
    }
  }

  public void close() {
    this.launcher.close();
  }

  /**
   * Run a job. Waits indefinitely for the job to complete.
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig) {
    return this.launcher.run(driverConfig);
  }

  /**
   * Run a job with a waiting timeout after which it will be killed, if it did not complete yet.
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @param timeOut      timeout on the job.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig, final long timeOut) {
    return this.launcher.run(driverConfig, timeOut);
  }

  @Override
  public String toString() {
    return this.launcher.toString();
  }

  /**
   * Instantiate a launcher for the given Configuration.
   *
   * @param runtimeConfiguration the resourcemanager configuration to be used
   * @return a DriverLauncher based on the given resourcemanager configuration
   * @throws com.microsoft.tang.exceptions.BindException      on configuration errors
   * @throws com.microsoft.tang.exceptions.InjectionException on configuration errors
   */
  public static TestDriverLauncher getLauncher(
      final Configuration runtimeConfiguration) throws BindException, InjectionException {

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, DriverLauncher.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, DriverLauncher.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, SilentRuntimeErrorHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, SilentFailedTestJobHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(TestDriverLauncher.class);
  }
}
