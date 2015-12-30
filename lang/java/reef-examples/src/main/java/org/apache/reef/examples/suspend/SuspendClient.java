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
package org.apache.reef.examples.suspend;

import org.apache.reef.client.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for suspend example.
 */
@Unit
public class SuspendClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(SuspendClient.class.getName());

  /**
   * Job Driver configuration.
   */
  private final Configuration driverConfig;

  /**
   * Reference to the REEF framework.
   */
  private final REEF reef;

  /**
   * Controller that listens for suspend/resume commands on a specified port.
   */
  private final SuspendClientControl controlListener;

  /**
   * @param reef                 reference to the REEF framework.
   * @param controlListener      suspend client control listener.
   * @param numCycles            number of cycles to run in the task.
   * @param delay                delay in seconds between cycles in the task.
   */
  @Inject
  SuspendClient(
      final REEF reef,
      final SuspendClientControl controlListener,
      @Parameter(Launch.NumCycles.class) final int numCycles,
      @Parameter(Launch.Delay.class) final int delay) throws BindException, IOException {

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Launch.NumCycles.class, Integer.toString(numCycles))
        .bindNamedParameter(Launch.Delay.class, Integer.toString(delay));

    cb.addConfiguration(DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SuspendDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "suspend-" + System.currentTimeMillis())
        .set(DriverConfiguration.ON_TASK_RUNNING, SuspendDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, SuspendDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_SUSPENDED, SuspendDriver.SuspendedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, SuspendDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SuspendDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SuspendDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, SuspendDriver.ClientMessageHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, SuspendDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, SuspendDriver.StopHandler.class)
        .build());

    this.driverConfig = cb.build();
    this.reef = reef;
    this.controlListener = controlListener;
  }

  /**
   * Start the job driver.
   */
  public void submit() {
    LOG.info("Start the job driver");
    this.reef.submit(this.driverConfig);
  }

  /**
   * Wait for the job to complete.
   */
  public void waitForCompletion() throws Exception {
    LOG.info("Waiting for the Job Driver to complete.");
    try {
      synchronized (this) {
        this.wait();
      }
    } catch (final InterruptedException ex) {
      LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);
    }
    this.reef.close();
    this.controlListener.close();
  }

  /**
   * Receive notification from the driver that the job is about to run.
   * RunningJob object is a proxy to the running job driver that can be used for sending messages.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "Running job: {0}", job.getId());
      SuspendClient.this.controlListener.setRunningJob(job);
    }
  }

  /**
   * Receive notification from the driver that the job had failed.
   * <p>
   * FailedJob is a proxy for the failed job driver
   * (contains job ID and exception thrown from the driver).
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOG.log(Level.SEVERE, "Failed job: " + job.getId(), job.getReason().orElse(null));
      synchronized (SuspendClient.this) {
        SuspendClient.this.notify();
      }
    }
  }

  /**
   * Receive notification from the driver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      synchronized (SuspendClient.this) {
        SuspendClient.this.notify();
      }
    }
  }

  /**
   * Receive notification that there was an exception thrown from the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "ERROR: " + error, error.getReason().orElse(null));
      synchronized (SuspendClient.this) {
        SuspendClient.this.notify();
      }
    }
  }
}
