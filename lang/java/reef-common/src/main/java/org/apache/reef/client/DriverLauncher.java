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
package org.apache.reef.client;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A launcher for REEF Drivers.
 * <p>
 * It can be instantiated using a configuration that can create a REEF instance.
 * For example, the local resourcemanager and the YARN resourcemanager can do this.
 * <p>
 * See {@link org.apache.reef.examples.hello} package for a demo use case.
 */
@Public
@Provided
@ClientSide
@Unit
public final class DriverLauncher {

  private static final Logger LOG = Logger.getLogger(DriverLauncher.class.getName());
  private final REEF reef;
  private LauncherStatus status = LauncherStatus.INIT;
  private RunningJob theJob = null;

  @Inject
  private DriverLauncher(final REEF reef) {
    this.reef = reef;
  }

  /**
   * Instantiate a launcher for the given Configuration.
   *
   * @param runtimeConfiguration the resourcemanager configuration to be used
   * @return a DriverLauncher based on the given resourcemanager configuration
   * @throws BindException      on configuration errors
   * @throws InjectionException on configuration errors
   */
  public static DriverLauncher getLauncher(
      final Configuration runtimeConfiguration) throws BindException, InjectionException {

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, RuntimeErrorHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(DriverLauncher.class);
  }

  /**
   * Kills the running job.
   */
  public synchronized void close() {
    if (this.status.isRunning()) {
      this.status = LauncherStatus.FORCE_CLOSED;
    }
    if (null != this.theJob) {
      this.theJob.close();
    }
    this.notify();
  }

  /**
   * Run a job. Waits indefinitely for the job to complete.
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig) {
    this.reef.submit(driverConfig);
    synchronized (this) {
      while (!this.status.isDone()) {
        try {
          LOG.log(Level.FINE, "Wait indefinitely");
          this.wait();
        } catch (final InterruptedException ex) {
          LOG.log(Level.FINE, "Interrupted: {0}", ex);
        }
      }
    }
    this.reef.close();
    synchronized (this) {
      return this.status;
    }
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
          if (waitTime <= 0) {
            break;
          }
          LOG.log(Level.FINE, "Wait for {0} milliSeconds", waitTime);
          this.wait(waitTime);
        } catch (final InterruptedException ex) {
          LOG.log(Level.FINE, "Interrupted: {0}", ex);
        }
      }
      if (System.currentTimeMillis() >= endTime) {
        LOG.log(Level.WARNING, "The Job timed out.");
        this.status = LauncherStatus.FORCE_CLOSED;
      }
    }

    this.reef.close();
    synchronized (this) {
      return this.status;
    }
  }

  /**
   * @return the current status of the job.
   */
  public LauncherStatus getStatus() {
    synchronized (this) {
      return this.status;
    }
  }

  /**
   * Update job status and notify the waiting thread.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public synchronized void setStatusAndNotify(final LauncherStatus status) {
    LOG.log(Level.FINEST, "Set status: {0} -> {1}", new Object[]{this.status, status});
    this.status = status;
    this.notify();
  }

  @Override
  public String toString() {
    return this.status.toString();
  }

  /**
   * Job driver notifies us that the job is running.
   */
  public final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "The Job {0} is running.", job.getId());
      theJob = job;
      setStatusAndNotify(LauncherStatus.RUNNING);
    }
  }

  /**
   * Job driver notifies us that the job had failed.
   */
  public final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      final Optional<Throwable> ex = job.getReason();
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), ex);
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(ex));
    }
  }

  /**
   * Job driver notifies us that the job had completed successfully.
   */
  public final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "The Job {0} is done.", job.getId());
      theJob = null;
      setStatusAndNotify(LauncherStatus.COMPLETED);
    }
  }

  /**
   * Handler an error in the job driver.
   */
  public final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Received a resourcemanager error", error.getReason());
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(error.getReason()));
    }
  }
}
