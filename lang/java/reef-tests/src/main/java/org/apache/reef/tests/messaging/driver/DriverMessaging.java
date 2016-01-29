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
package org.apache.reef.tests.messaging.driver;

import org.apache.reef.client.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runner for DriverMessagingTest.
 */
@Unit
public final class DriverMessaging {

  private static final Logger LOG = Logger.getLogger(DriverMessaging.class.getName());

  private final REEF reef;

  private String lastMessage = null;
  private Optional<RunningJob> theJob = Optional.empty();
  private LauncherStatus status = LauncherStatus.INIT;

  @Inject
  private DriverMessaging(final REEF reef) {
    this.reef = reef;
  }

  public static LauncherStatus run(final Configuration runtimeConfiguration,
                                   final int launcherTimeout) throws BindException, InjectionException {

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, DriverMessaging.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, DriverMessaging.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, DriverMessaging.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, DriverMessaging.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, DriverMessaging.RuntimeErrorHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(DriverMessaging.class).run(launcherTimeout, 1000);
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

    final Configuration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "DriverMessagingTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, DriverMessagingDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverMessagingDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverMessagingDriver.ClientMessageHandler.class)
        .build();


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
    synchronized (this) {
      return this.status;
    }
  }

  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      final String msg = new String(message.get(), StandardCharsets.UTF_8);
      synchronized (DriverMessaging.this) {
        if (!msg.equals(DriverMessaging.this.lastMessage)) {
          LOG.log(Level.SEVERE, "Expected {0} but got {1}",
              new Object[]{DriverMessaging.this.lastMessage, msg});
          DriverMessaging.this.status = LauncherStatus.FAILED;
          DriverMessaging.this.notify();
        }
      }
    }
  }

  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "The Job {0} is running", job.getId());
      synchronized (DriverMessaging.this) {
        DriverMessaging.this.status = LauncherStatus.RUNNING;
        DriverMessaging.this.theJob = Optional.of(job);
        DriverMessaging.this.lastMessage = "Hello, REEF!";
        DriverMessaging.this.theJob.get().send(DriverMessaging.this.lastMessage.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "Job Completed: {0}", job);
      synchronized (DriverMessaging.this) {
        DriverMessaging.this.status = LauncherStatus.COMPLETED;
        DriverMessaging.this.notify();
      }
    }
  }

  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), job.getReason().orElse(null));
      synchronized (DriverMessaging.this) {
        DriverMessaging.this.status = LauncherStatus.failed(job.getReason());
        DriverMessaging.this.notify();
      }
    }
  }

  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Received a runtime error: " + error, error.getReason().orElse(null));
      synchronized (DriverMessaging.this) {
        DriverMessaging.this.status = LauncherStatus.failed(error.getReason());
        DriverMessaging.this.notify();
      }
    }
  }
}
