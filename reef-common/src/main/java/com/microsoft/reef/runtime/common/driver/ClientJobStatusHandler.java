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
package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobControlProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.JobStatusProto;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends JobStatus reports to the client side.
 */
@DriverSide
@Unit
public final class ClientJobStatusHandler implements JobMessageObserver {

  private final static Logger LOG = Logger.getLogger(ClientJobStatusHandler.class.getName());

  private final RuntimeClock clock;

  private final String jobID;

  private final EventHandler<JobStatusProto> jobStatusHandler;

  private final AutoCloseable jobControlChannel;

  @Inject
  public ClientJobStatusHandler(
      final RemoteManager remoteManager, final RuntimeClock clock,
      final @Parameter(DriverRuntimeConfigurationOptions.JobControlHandler.class) EventHandler<JobControlProto> jobControlHandler,
      final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobID,
      final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID) {
    this.clock = clock;
    this.jobID = jobID;

    // Get a handler for sending job status messages to the client
    this.jobStatusHandler = remoteManager.getHandler(clientRID, JobStatusProto.class);
    this.jobControlChannel = remoteManager.registerHandler(clientRID, JobControlProto.class, jobControlHandler);
  }

  void close(final Optional<Throwable> exception) {
    try {
      if (exception.isPresent()) {
        onError(exception.get());
      } else {
        // Note: Wake will throw an exception if the client has closed the channel; simply ignoring this fact is fine.
        send(JobStatusProto.newBuilder()
            .setIdentifier(ClientJobStatusHandler.this.jobID.toString())
            .setState(ReefServiceProtos.State.DONE)
            .build());
      }
    } catch (final Throwable t) {
      LOG.warning(t.toString());
    }

    try {
      ClientJobStatusHandler.this.jobControlChannel.close();
    } catch (Exception e) {
      LOG.warning(e.toString());
    }
  }

  @Override
  public void onNext(final byte[] message) {
    LOG.log(Level.INFO, "Job message from {0}", this.jobID);
    send(JobStatusProto.newBuilder()
        .setIdentifier(this.jobID.toString())
        .setState(ReefServiceProtos.State.RUNNING)
        .setMessage(ByteString.copyFrom(message))
        .build());
  }

  @Override
  public void onError(final Throwable exception) {
    LOG.log(Level.SEVERE, "Job exception", exception);
    final ObjectSerializableCodec<Throwable> codec = new ObjectSerializableCodec<>();
    send(JobStatusProto.newBuilder()
        .setIdentifier(this.jobID.toString())
        .setState(ReefServiceProtos.State.FAILED)
        .setException(ByteString.copyFrom(codec.encode(exception)))
        .build());
    this.clock.close();
  }

  /**
   * Send job status proto to the client via Wake
   * based remote event handler
   *
   * @param status of the job
   */
  private void send(final JobStatusProto status) {
    LOG.log(Level.INFO, "Sending job status: {0}", status);
    this.jobStatusHandler.onNext(status);
  }

  /**
   * Wake based event handler for sending job messages to the client
   */
  public final class JobMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      onNext(message);
    }
  }

  /**
   * Wake based event handler for sending job exceptions to the client
   */
  public final class JobExceptionHandler implements EventHandler<Exception> {
    @Override
    public void onNext(final Exception exception) {
      onError(exception);
    }
  }

  public final class RuntimeStartHandler implements EventHandler<RuntimeStart> {
    @Override
    public void onNext(final RuntimeStart runtimeStart) {
      LOG.log(Level.INFO, "Processing runtimeStart: {0}", runtimeStart);
      send(JobStatusProto.newBuilder()
          .setIdentifier(ClientJobStatusHandler.this.jobID.toString())
          .setState(ReefServiceProtos.State.INIT)
          .build());
    }
  }
}
