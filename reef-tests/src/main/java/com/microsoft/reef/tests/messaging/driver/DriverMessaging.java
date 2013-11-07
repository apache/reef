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
package com.microsoft.reef.tests.messaging.driver;

import com.microsoft.reef.client.*;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.RuntimeError;
import com.microsoft.reef.utils.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DriverMessaging implements JobObserver, RuntimeErrorHandler {

  private static final Logger LOG = Logger.getLogger(DriverMessaging.class.getName());

  private final REEF reef;

  private String lastMessage = null;
  private Optional<RunningJob> theJob = Optional.empty();

  private LauncherStatus status = LauncherStatus.INIT;

  @Inject
  private DriverMessaging(final REEF reef) {
    this.reef = reef;
  }

  @Override
  public synchronized void onNext(final JobMessage message) {
    final String msg = new String(message.get());
    if (!msg.equals(this.lastMessage)) {
      LOG.log(Level.SEVERE, "Expected '{0}' but got '{1}", new Object[] { this.lastMessage, msg });
      this.status = LauncherStatus.FAILED;
      this.notify();
    }
  }

  @Override
  public void onNext(final RunningJob job) {
    LOG.log(Level.INFO, "The Job {0} is running", job.getId());
    this.status = LauncherStatus.RUNNING;
    this.theJob = Optional.of(job);
    this.lastMessage = "Hello, REEF!";
    this.theJob.get().send(this.lastMessage.getBytes());
  }

  @Override
  public synchronized void onNext(final CompletedJob job) {
    LOG.log(Level.INFO, "Job Completed: {0}", job);
    this.status = LauncherStatus.COMPLETED;
    this.notify();
  }

  @Override
  public synchronized void onError(final FailedJob job) {
    LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), job.getJobException());
    this.status = LauncherStatus.FAILED(job.getJobException());
    this.notify();
  }

  @Override
  public synchronized void onError(final RuntimeError error) {
    LOG.log(Level.SEVERE, "Received a runtime error: " + error, error.getException());
    this.status = LauncherStatus.FAILED(error.getException());
    this.notify();
  }

  public synchronized void close() {
    if (this.status.isRunning()) {
      this.status = LauncherStatus.FORCE_CLOSED;
    }
    if (this.theJob.isPresent()) {
      this.theJob.get().close();
    }
    this.notify();
  }

  private LauncherStatus run(final long jobTimeout, final long statusTimeout) {

    final long startTime = System.currentTimeMillis();
    LOG.log(Level.INFO, "Submitting REEF Job");

    final Configuration driverConfig;
    try {
      driverConfig =
          EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "DriverMessagingTest")
            .set(DriverConfiguration.ON_DRIVER_STARTED, DriverMessagingDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverMessagingDriver.AllocatedEvaluatorHandler.class)
            .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverMessagingDriver.ClientMessageHandler.class)
          .build();
    } catch (final BindException ex) {
      throw new RuntimeException(ex);
    }

    this.reef.submit(driverConfig);

    synchronized (this) {
      while (!this.status.isDone()) {
        LOG.log(Level.INFO, "Waiting for REEF job to finish.");
        try {
          this.wait(statusTimeout);
        } catch (final InterruptedException ex) {
          LOG.log(Level.FINER, "Waiting for REEF job interrupted.", ex);
        }
        if (System.currentTimeMillis() - startTime >= jobTimeout) {
          LOG.log(Level.INFO, "Waiting for REEF job timed out after {0} sec.",
              (System.currentTimeMillis() - startTime) / 1000);
          break;
        }
      }
    }

    this.reef.close();
    return this.status;
  }

  public static LauncherStatus run(final Configuration runtimeConfiguration,
                                   final int launcherTimeout) throws BindException, InjectionException {

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.JOB_OBSERVER, DriverMessaging.class)
        .set(ClientConfiguration.RUNTIME_ERROR_HANDLER, DriverMessaging.class)
        .build();

    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(DriverMessaging.class).run(launcherTimeout, 1000);
  }
}
