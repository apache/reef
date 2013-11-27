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
package com.microsoft.reef.runtime.common.client;

import com.google.protobuf.ByteString;
import com.microsoft.reef.client.*;
import com.microsoft.reef.exception.JobException;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobControlProto;
import com.microsoft.reef.proto.ClientRuntimeProtocol.Signal;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.JobStatusProto;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;

public class RunningJobImpl implements RunningJob, EventHandler<JobStatusProto> {

  private static final Logger LOG = Logger.getLogger(RunningJob.class.getName());

  private final String jobId;
  private final EventHandler<JobControlProto> jobControlHandler;

  private final EventHandler<RunningJob> runningJobEventHandler;
  private final EventHandler<CompletedJob> completedJobEventHandler;
  private final EventHandler<FailedJob> failedJobEventHandler;
  private final EventHandler<JobMessage> jobMessageEventHandler;

  @Inject
  RunningJobImpl(final RemoteManager remoteManager, final JobStatusProto status,
      final @Parameter(ClientManager.DriverRemoteIdentifier.class) String driverRID,
      final @Parameter(ClientConfigurationOptions.RunningJobHandler.class) EventHandler<RunningJob> runningJobEventHandler,
      final @Parameter(ClientConfigurationOptions.CompletedJobHandler.class) EventHandler<CompletedJob> completedJobEventHandler,
      final @Parameter(ClientConfigurationOptions.FailedJobHandler.class) EventHandler<FailedJob> failedJobEventHandler,
      final @Parameter(ClientConfigurationOptions.JobMessageHandler.class) EventHandler<JobMessage> jobMessageEventHandler) {

    this.jobId = status.getIdentifier();

    this.runningJobEventHandler = runningJobEventHandler;
    this.completedJobEventHandler = completedJobEventHandler;
    this.failedJobEventHandler = failedJobEventHandler;
    this.jobMessageEventHandler = jobMessageEventHandler;
    this.jobControlHandler = remoteManager.getHandler(driverRID, JobControlProto.class);

    this.runningJobEventHandler.onNext(this);
    this.onNext(status);
  }

  @Override
  public void close() {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setSignal(Signal.SIG_TERMINATE)
            .build());
  }

  @Override
  public void close(final byte[] message) {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setSignal(Signal.SIG_TERMINATE)
            .setMessage(ByteString.copyFrom(message))
            .build());
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public void send(byte[] message) {
    this.jobControlHandler.onNext(
        JobControlProto.newBuilder()
            .setIdentifier(this.jobId)
            .setMessage(ByteString.copyFrom(message))
            .build());
  }

  @Override
  public void onNext(final JobStatusProto value) {

    final ReefServiceProtos.State state = value.getState();
    LOG.log(Level.FINEST,  "Received job status: {0} from {1}",
            new Object[] { state, value.getIdentifier() });

    if (value.hasMessage()) {
      this.jobMessageEventHandler.onNext(
          new JobMessage(getId(), value.getMessage().toByteArray()));
    }

    if (state == ReefServiceProtos.State.DONE) {

      this.completedJobEventHandler.onNext(new CompletedJob() {
        @Override
        public String getId() {
          return RunningJobImpl.this.jobId;
        }

        @Override
        public String toString() {
          return "CompletedJob{" + "jobId=" + getId() + '}';
        }
      });

    } else if (state == ReefServiceProtos.State.FAILED) {

      final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();

      final JobException error = value.hasException() ?
          new JobException(this.jobId, codec.decode(value.getException().toByteArray())) :
          new JobException(this.jobId, "Unknown failure cause");

      this.failedJobEventHandler.onNext(new FailedJob(this.jobId, error));
    }
  }

  @Override
  public String toString() {
    return "RunningJobImpl{" + "jobId=" + this.jobId + '}';
  }
}
