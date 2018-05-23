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
package org.apache.reef.runtime.common.client;

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.CompletedJob;
import org.apache.reef.client.FailedJob;
import org.apache.reef.client.JobMessage;
import org.apache.reef.client.RunningJob;
import org.apache.reef.client.parameters.JobCompletedHandler;
import org.apache.reef.client.parameters.JobFailedHandler;
import org.apache.reef.client.parameters.JobMessageHandler;
import org.apache.reef.client.parameters.JobRunningHandler;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.proto.ClientRuntimeProtocol.JobControlProto;
import org.apache.reef.proto.ClientRuntimeProtocol.Signal;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.proto.ReefServiceProtos.JobStatusProto;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of RunningJob.
 */
@ClientSide
@Private
public final class RunningJobImpl implements RunningJob, EventHandler<JobStatusProto> {

  private static final Logger LOG = Logger.getLogger(RunningJob.class.getName());

  private final String jobId;

  private final EventHandler<JobControlProto> jobControlHandler;
  private final EventHandler<RunningJob> runningJobEventHandler;
  private final EventHandler<CompletedJob> completedJobEventHandler;
  private final EventHandler<FailedJob> failedJobEventHandler;
  private final EventHandler<JobMessage> jobMessageEventHandler;
  private final ExceptionCodec exceptionCodec;

  @Inject
  RunningJobImpl(final RemoteManager remoteManager,
                 @Parameter(DriverIdentifier.class) final String driverIdentifier,
                 @Parameter(REEFImplementation.DriverRemoteIdentifier.class) final String driverRID,
                 @Parameter(JobRunningHandler.class) final EventHandler<RunningJob> runningJobEventHandler,
                 @Parameter(JobCompletedHandler.class) final EventHandler<CompletedJob> completedJobEventHandler,
                 @Parameter(JobFailedHandler.class) final EventHandler<FailedJob> failedJobEventHandler,
                 @Parameter(JobMessageHandler.class) final EventHandler<JobMessage> jobMessageEventHandler,
                 final ExceptionCodec exceptionCodec) {

    this.jobId = driverIdentifier;
    this.runningJobEventHandler = runningJobEventHandler;
    this.completedJobEventHandler = completedJobEventHandler;
    this.failedJobEventHandler = failedJobEventHandler;
    this.jobMessageEventHandler = jobMessageEventHandler;
    this.exceptionCodec = exceptionCodec;
    this.jobControlHandler = remoteManager.getHandler(driverRID, JobControlProto.class);

    this.runningJobEventHandler.onNext(this);
    LOG.log(Level.FINE, "Instantiated 'RunningJobImpl'");
  }

  @Override
  public synchronized void close() {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setSignal(Signal.SIG_TERMINATE)
            .build()
    );
  }

  @Override
  public synchronized void close(final byte[] message) {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setSignal(Signal.SIG_TERMINATE)
            .setMessage(ByteString.copyFrom(message))
            .build()
    );
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public synchronized void send(final byte[] message) {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setMessage(ByteString.copyFrom(message))
            .build()
    );
  }

  @Override
  public synchronized void onNext(final JobStatusProto value) {

    final ReefServiceProtos.State state = value.getState();
    LOG.log(Level.FINEST, "Received job status: {0} from {1}",
        new Object[]{state, value.getIdentifier()});

    if (value.hasMessage()) {
      this.jobMessageEventHandler.onNext(
          new JobMessage(getId(), value.getMessage().toByteArray()));
    }
    if (state == ReefServiceProtos.State.DONE) {
      this.completedJobEventHandler.onNext(new CompletedJobImpl(this.getId()));
    } else if (state == ReefServiceProtos.State.FAILED) {
      this.onJobFailure(value);
    }
  }

  /**
   * Inform the client of a failed job.
   *
   * @param jobStatusProto status of the failed job
   */
  private synchronized void onJobFailure(final JobStatusProto jobStatusProto) {
    assert jobStatusProto.getState() == ReefServiceProtos.State.FAILED;

    final String id = this.jobId;
    final Optional<byte[]> data = jobStatusProto.hasException() ?
        Optional.of(jobStatusProto.getException().toByteArray()) :
        Optional.<byte[]>empty();
    final Optional<Throwable> cause = this.exceptionCodec.fromBytes(data);

    final String message;
    if (cause.isPresent() && cause.get().getMessage() != null) {
      message = cause.get().getMessage();
    } else {
      message = "No Message sent by the Job in exception " + cause.get().toString();
      LOG.log(Level.WARNING, message, cause.get());
    }
    final Optional<String> description = Optional.of(message);

    final FailedJob failedJob = new FailedJob(id, message, description, cause, data);
    this.failedJobEventHandler.onNext(failedJob);
  }

  @Override
  public String toString() {
    return "RunningJob{'" + this.jobId + "'}";
  }
}
