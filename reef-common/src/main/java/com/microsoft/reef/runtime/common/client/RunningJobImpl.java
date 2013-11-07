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

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunningJobImpl implements RunningJob, EventHandler<JobStatusProto> {

  private static final Logger LOG = Logger.getLogger(RunningJob.class.toString());


  private final String jobid;
  private final JobObserver jobObserver;
  private final EventHandler<JobControlProto> jobControlHandler;

  @Inject
  RunningJobImpl(final RemoteManager remoteManager, final JobObserver jobObserver, final JobStatusProto status,
                 @Parameter(ClientManager.DriverRemoteIdentifier.class) final String driverRID) {
    this.jobid = status.getIdentifier();
    this.jobObserver = jobObserver;

    this.jobControlHandler = remoteManager.getHandler(driverRID, JobControlProto.class);

    this.jobObserver.onNext(this);
    onNext(status);
  }

  @Override
  public void close() {
    this.jobControlHandler.onNext(JobControlProto.newBuilder().setIdentifier(this.jobid.toString()).setSignal(Signal.SIG_TERMINATE).build());
  }

  @Override
  public void close(final byte[] message) {
    this.jobControlHandler.onNext(JobControlProto.newBuilder().setIdentifier(this.jobid.toString()).setSignal(Signal.SIG_TERMINATE).setMessage(ByteString.copyFrom(message)).build());
  }

  @Override
  public String getId() {
    return this.jobid;
  }

  @Override
  public void send(byte[] message) {
    this.jobControlHandler.onNext(JobControlProto.newBuilder().setIdentifier(this.jobid.toString()).setMessage(ByteString.copyFrom(message)).build());
  }

  @Override
  public void onNext(JobStatusProto value) {
    try {
      final ReefServiceProtos.State state = value.getState();

      if (value.hasMessage()) {
        this.jobObserver.onNext(new JobMessage(getId(), value.getMessage().toByteArray()));
      }

      if (state == ReefServiceProtos.State.DONE) {
        Logger.getLogger(RunningJobImpl.class.getName()).log(Level.INFO, "Received a JobStatus.DONE");

        this.jobObserver.onNext(new CompletedJob() {
          @Override
          public String getId() {
            return RunningJobImpl.this.jobid;
          }

          @Override
          public String toString() {
            return "CompletedJob{" + "jobid=" + getId() + '}';
          }
        });
      } else if (state == ReefServiceProtos.State.FAILED) {
        final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
        final JobException exception = value.hasException() ? new JobException(this.jobid, codec.decode(value.getException().toByteArray()))
            : new JobException(this.jobid, "Unknown failure cause");
        this.jobObserver.onError(new FailedJob() {
          @Override
          public JobException getJobException() {
            return exception;
          }

          @Override
          public String getId() {
            return RunningJobImpl.this.jobid;
          }
        });
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public String toString() {
    return "RunningJobImpl{" + "jobid=" + jobid + '}';
  }
}
