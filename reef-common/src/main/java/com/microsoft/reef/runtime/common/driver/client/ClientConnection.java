package com.microsoft.reef.runtime.common.driver.client;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Represents the communication channel to the client.
 */
@DriverSide
public final class ClientConnection {
  private final EventHandler<ReefServiceProtos.JobStatusProto> jobStatusHandler;

  @Inject
  public ClientConnection(final RemoteManager remoteManager,
                          final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID) {
    this.jobStatusHandler = remoteManager.getHandler(clientRID, ReefServiceProtos.JobStatusProto.class);
  }

  /**
   * Send the given JobStatus to the client.
   *
   * @param status
   */
  public synchronized void send(final ReefServiceProtos.JobStatusProto status) {
    this.jobStatusHandler.onNext(status);
  }
}
