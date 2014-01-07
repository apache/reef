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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
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
 * A launcher for REEF Drivers.
 * <p/>
 * It can be instantiated using a configuration that can create a REEF instance.
 * For example, the local runtime and the YARN runtime can do this.
 * <p/>
 * {@see com.microsoft.reef.examples.hello.HelloREEF} for a demo use case.
 */
@Public
@Provided
@ClientSide
@Unit
public final class DriverLauncher {

  private static final Logger LOG = Logger.getLogger(DriverLauncher.class.getName());

  private LauncherStatus status = LauncherStatus.INIT;

  private RunningJob theJob = null;

  private final REEF reef;

  @Inject
  private DriverLauncher(final REEF reef) {
    this.reef = reef;
  }

  /**
   * Job driver notifies us that the job is running.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "The Job {0} is running.", job.getId());
      setStatusAndNotify(LauncherStatus.RUNNING);
    }
  }

  /**
   * Job driver notifies us that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      final Throwable ex = job.getCause();
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), ex);
      setStatusAndNotify(LauncherStatus.FAILED(ex));
    }
  }

  /**
   * Job driver notifies us that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "The Job {0} is done.", job);
      setStatusAndNotify(LauncherStatus.COMPLETED);
    }
  }

  /**
   * Handler an error in the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Received a runtime error", error.getCause());
      setStatusAndNotify(LauncherStatus.FAILED(error.getCause()));
    }
  }

  /**
   * Kills the running job.
   */
  public void close() {
    synchronized (this) {
      if (this.status.isRunning()) {
        this.status = LauncherStatus.FORCE_CLOSED;
      }
      if (null != this.theJob) {
        this.theJob.close();
      }
      this.notify();
    }
  }

  /**
   * Run a job
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig) {

    this.reef.submit(driverConfig);
    synchronized (this) {
      while (!this.status.isDone()) {
        try {
          this.wait();
        } catch (final InterruptedException ex) {
        }
      }
    }
    this.reef.close();
    return this.status;
  }


  /**
   * Run a job with a waiting timeout after which it will be killed, if it did not complete yet.
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @param timeOut      timeout on the job.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig, final long timeOut) {
    final long endTime = System.currentTimeMillis() + timeOut;
    this.reef.submit(driverConfig);
    synchronized (this) {
      while (!this.status.isDone()) {
        try {
          final long waitTime = endTime - System.currentTimeMillis();
          if (waitTime > 0) {
            this.wait(waitTime);
              LOG.log(Level.FINE, "Waited for " + waitTime + " milliSeconds");
          } else {
            break;
          }
        } catch (final InterruptedException ex) {
        }
      }
    }

    this.reef.close();
    return this.status;
  }

  /**
   * Instantiate a launcher for the given Configuration.
   *
   * @param runtimeConfiguration the runtime configuration to be used
   * @return a DriverLauncher based on the given runtime configuration
   * @throws BindException      on configuration errors
   * @throws InjectionException on configuration errors
   */
  public static DriverLauncher getLauncher(final Configuration runtimeConfiguration) throws BindException, InjectionException {
    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, DriverLauncher.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, DriverLauncher.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, DriverLauncher.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, DriverLauncher.RuntimeErrorHandler.class)
        .build();
    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(DriverLauncher.class);
  }

  /**
   * @return the current status of the job.
   */
  public LauncherStatus getStatus() {
    return this.status;
  }

  /**
   * Update job status and notify the waiting thread.
   */
  private synchronized void setStatusAndNotify(final LauncherStatus status) {
    this.status = status;
    this.notify();
  }

  @Override
  public String toString() {
    return this.status.toString();
  }
}
