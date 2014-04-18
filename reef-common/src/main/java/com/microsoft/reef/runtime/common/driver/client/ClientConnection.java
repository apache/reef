package com.microsoft.reef.runtime.common.driver.client;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the communication channel to the client.
 */
@DriverSide
public final class ClientConnection {

  private static final Logger LOG = Logger.getLogger(ClientConnection.class.getName());

  private final EventHandler<ReefServiceProtos.JobStatusProto> jobStatusHandler;
  private final String jobIdentifier;

  @Inject
  public ClientConnection(
      final RemoteManager remoteManager,
      final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID,
      final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobIdentifier) {
    this.jobIdentifier = jobIdentifier;
    if (clientRID.equals(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.NONE)) {
      LOG.log(Level.INFO, "Unable to establish a connection with the client");
      this.jobStatusHandler = new LoggingJobStatusHandler();
    } else {
      this.jobStatusHandler = remoteManager.getHandler(clientRID, ReefServiceProtos.JobStatusProto.class);
      LOG.log(Level.INFO, "Instantiated 'ClientConnection'");
    }
  }

  /**
   * Send the given JobStatus to the client.
   *
   * @param status
   */
  public synchronized void send(final ReefServiceProtos.JobStatusProto status) {
    this.jobStatusHandler.onNext(status);
  }

  /**
   * Send the given byte[] as a message to the client.
   *
   * @param message
   */
  public synchronized void sendMessage(final byte[] message) {
    this.send(ReefServiceProtos.JobStatusProto.newBuilder()
        .setIdentifier(this.jobIdentifier)
        .setState(ReefServiceProtos.State.RUNNING)
        .setMessage(ByteString.copyFrom(message))
        .build());
  }
}

