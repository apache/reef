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
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.client.parameters.ResourceManagerErrorHandler;
import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
@Private
final class RunningJobsImpl implements RunningJobs {
  private static final Logger LOG = Logger.getLogger(RunningJobsImpl.class.getName());
  private final Map<String, RunningJobImpl> jobs = new HashMap<>();
  private final Injector injector;
  private final InjectionFuture<EventHandler<FailedRuntime>> failedRuntimeEventHandler;

  @Inject
  RunningJobsImpl(final Injector injector,
                  final @Parameter(ResourceManagerErrorHandler.class) InjectionFuture<EventHandler<FailedRuntime>> failedRuntimeEventHandler) {
    this.injector = injector;
    this.failedRuntimeEventHandler = failedRuntimeEventHandler;
    LOG.log(Level.FINE, "Instantiated 'RunningJobImpl'");
  }


  @Override
  public synchronized void closeAllJobs() {
    for (final RunningJobImpl runningJob : this.jobs.values()) {
      LOG.log(Level.WARNING, "Force close job {0}", runningJob.getId());
      runningJob.close();
    }
  }

  @Override
  public synchronized void onJobStatusMessage(final RemoteMessage<ReefServiceProtos.JobStatusProto> message) {
    final ReefServiceProtos.JobStatusProto status = message.getMessage();
    final String jobIdentifier = status.getIdentifier();
    LOG.log(Level.FINE, "Processing message from Job: " + jobIdentifier);

    if (status.getState() == ReefServiceProtos.State.INIT) {
      try {
        final RunningJobImpl runningJob = this.newRunningJob(status.getIdentifier(), message.getIdentifier().toString());
        this.put(runningJob);
      } catch (final BindException | InjectionException configError) {
        throw new RuntimeException("Configuration error for: " + status, configError);
      }
    }

    this.get(jobIdentifier).onNext(status);
    if ((status.getState() != ReefServiceProtos.State.RUNNING) &&
        (status.getState() != ReefServiceProtos.State.INIT)) {
      this.remove(status.getIdentifier());
    }
    LOG.log(Level.FINE, "Done processing message from Job " + jobIdentifier);
  }

  @Override
  public synchronized void onRuntimeErrorMessage(RemoteMessage<ReefServiceProtos.RuntimeErrorProto> runtimeFailure) {
    try {
      this.remove(runtimeFailure.getMessage().getIdentifier());
    } finally {
      this.failedRuntimeEventHandler.get().onNext(new FailedRuntime(runtimeFailure.getMessage()));
    }
  }


  /**
   * A guarded get() that throws an exception if the RunningJob isn't known
   *
   * @param jobIdentifier
   * @return
   */
  private synchronized RunningJobImpl get(final String jobIdentifier) {
    final RunningJobImpl result = this.jobs.get(jobIdentifier);
    if (null == result) {
      throw new RuntimeException("Trying to get a RunningJob that is unknown: " + jobIdentifier);
    }
    return result;
  }

  /**
   * A guarded remove() that throws an exception if no RunningJob is known for this id.
   *
   * @param jobIdentifier
   */
  private synchronized void remove(final String jobIdentifier) {
    final RunningJobImpl result = this.jobs.remove(jobIdentifier);
    if (null == result) {
      throw new RuntimeException("Trying to remove a RunningJob that is unknown: " + jobIdentifier);
    }
  }


  private synchronized void put(final RunningJobImpl runningJob) {
    final String jobIdentifier = runningJob.getId();
    if (this.jobs.containsKey(jobIdentifier)) {
      throw new IllegalStateException("Trying to re-add a job that is already known: " + jobIdentifier);
    }
    LOG.log(Level.FINE, "Adding Job with ID: " + jobIdentifier);
    this.jobs.put(jobIdentifier, runningJob);
  }

  /**
   * @param jobIdentifier
   * @param remoteIdentifier
   * @return
   * @throws BindException
   * @throws InjectionException
   */
  private synchronized RunningJobImpl newRunningJob(final String jobIdentifier, final String remoteIdentifier) throws BindException, InjectionException {
    final Injector child = this.injector.forkInjector();
    child.bindVolatileParameter(REEFImplementation.DriverRemoteIdentifier.class, remoteIdentifier);
    child.bindVolatileParameter(DriverIdentifier.class, jobIdentifier);
    return child.getInstance(RunningJobImpl.class);
  }
}
