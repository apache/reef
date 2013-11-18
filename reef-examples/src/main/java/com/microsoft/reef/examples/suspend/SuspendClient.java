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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.client.*;
import com.microsoft.reef.util.RuntimeError;
import com.microsoft.reef.utils.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class SuspendClient implements JobObserver {

  /**
   * Standard java logger.
   */
  private final static Logger LOG = Logger.getLogger(SuspendClient.class.getName());

  /**
   * Job Driver configuration.
   */
  private final Configuration driverConfig;

  /**
   * Reference to the REEF framework.
   */
  private final REEF reef;

  private final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

  /**
   * A reference to the running job that allows client to send messages back to the job driver
   */
  private RunningJob runningJob;

  /**
   * Controller that listens for suspend/resume commands on a specified port.
   */
  private final SuspendClientControl controlListener;

  /**
   * @param reef      reference to the REEF framework.
   * @param port      port to listen to for suspend/resume commands.
   * @param numCycles number of cycles to run in the activity.
   * @param delay     delay in seconds between cycles in the activity.
   */
  @Inject
  SuspendClient(final REEF reef,
                @Parameter(SuspendClientControl.Port.class) final int port,
                @Parameter(Launch.NumCycles.class) final int numCycles,
                @Parameter(Launch.Delay.class) final int delay) throws BindException {

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(Launch.NumCycles.class, Integer.toString(numCycles));
    cb.bindNamedParameter(Launch.Delay.class, Integer.toString(delay));

    cb.addConfiguration(
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "suspend-" + System.currentTimeMillis())
          .set(DriverConfiguration.ON_ACTIVITY_RUNNING, SuspendDriver.RunningActivityHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_COMPLETED, SuspendDriver.CompletedActivityHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_SUSPENDED, SuspendDriver.SuspendedActivityHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_MESSAGE, SuspendDriver.ActivityMessageHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SuspendDriver.AllocatedEvaluatorHandler.class)
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SuspendDriver.ActiveContextHandler.class)
          .set(DriverConfiguration.ON_CLIENT_MESSAGE, SuspendDriver.ClientMessageHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STARTED, SuspendDriver.StartHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STOP, SuspendDriver.StopHandler.class)
        .build());

    this.driverConfig = cb.build();
    this.reef = reef;
    this.controlListener = new SuspendClientControl(port);
  }

  /**
   * Start the job driver.
   */
  public void submit() {
    LOG.info("Start the job driver");
    this.reef.submit(this.driverConfig);
  }

  /**
   * Receive message from the driver.
   * This method is inherited from the JobObserver interface.
   *
   * @param message message from the job driver.
   */
  @Override
  public synchronized void onNext(final JobMessage message) {
    final String msgString = this.codec.decode(message.get());
    LOG.log(Level.INFO, "Driver: {0} -> Client: {1}", new Object[]{message.getId(), msgString});
  }

  /**
   * Receive notification from the driver that the job is about to run.
   * This method is inherited from the JobObserver interface.
   *
   * @param job a proxy to the running job driver that can be used for sending messages.
   */
  @Override
  public void onNext(final RunningJob job) {
    LOG.log(Level.INFO, "Running job: {0}", job.getId());
    this.runningJob = job;
    this.controlListener.setRunningJob(job);
  }

  /**
   * Receive notification from the driver that the job had failed.
   * This method is inherited from the JobObserver interface.
   *
   * @param job failed job driver (contains job ID and exception thrown from the driver).
   */
  @Override
  public synchronized void onError(final FailedJob job) {
    LOG.log(Level.SEVERE, "Failed job: " + job.getId(), job.getJobException());
    this.notify();
  }

  /**
   * Receive notification from the driver that the job had completed successfully.
   * This method is inherited from the JobObserver interface.
   *
   * @param job completed job driver (has job ID).
   */
  @Override
  public synchronized void onNext(final CompletedJob job) {
    LOG.log(Level.INFO, "Completed job: {0}", job.getId());
    this.notify();
  }

  /**
   * Receive notification that there was an exception thrown from the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<RuntimeError> {
    @Override
    public void onNext(final RuntimeError error) {
      LOG.log(Level.SEVERE, "ERROR: " + error, error.getException());
      synchronized (SuspendClient.class) {
        SuspendClient.this.notify();
      }
    }
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
}
