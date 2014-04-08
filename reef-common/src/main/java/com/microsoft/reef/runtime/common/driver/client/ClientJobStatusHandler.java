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
package com.microsoft.reef.runtime.common.driver.client;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobControlProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.JobStatusProto;
import com.microsoft.reef.runtime.common.driver.DriverRuntimeConfigurationOptions;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

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

  private final String jobID;

  private final ClientConnection clientConnection;

  private final DriverStatusManager driverStatusManager;

  @Inject
  public ClientJobStatusHandler(
      final RemoteManager remoteManager,
      final @Parameter(DriverRuntimeConfigurationOptions.JobControlHandler.class) EventHandler<JobControlProto> jobControlHandler,
      final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobID,
      final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID,
      final ClientConnection clientConnection,
      final DriverStatusManager driverStatusManager) {

    this.jobID = jobID;
    this.clientConnection = clientConnection;
    this.driverStatusManager = driverStatusManager;

    // Get a handler for sending job status messages to the client
    remoteManager.registerHandler(clientRID, JobControlProto.class, jobControlHandler);
  }


  @Override
  public synchronized void onNext(final byte[] message) {
    LOG.log(Level.FINEST, "Job message from {0}", this.jobID);
    this.clientConnection.send(JobStatusProto.newBuilder()
        .setIdentifier(this.jobID.toString())
        .setState(ReefServiceProtos.State.RUNNING)
        .setMessage(ByteString.copyFrom(message))
        .build());
  }

  @Override
  public synchronized void onError(final Throwable exception) {
    this.driverStatusManager.onError(exception);
  }


  /**
   * Wake based event handler for sending job messages to the client
   */
  public final class JobMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      ClientJobStatusHandler.this.onNext(message);
    }
  }

  /**
   * Wake based event handler for sending job exceptions to the client
   */
  public final class JobExceptionHandler implements EventHandler<Exception> {
    @Override
    public void onNext(final Exception exception) {
      ClientJobStatusHandler.this.onError(exception);
    }
  }
}
