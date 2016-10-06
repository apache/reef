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
package org.apache.reef.runtime.common.driver.client;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic interface for job status messages' handler.
 */
@DriverSide
final class RemoteClientJobStatusHandler implements JobStatusHandler {

  private static final Logger LOG = Logger.getLogger(RemoteClientJobStatusHandler.class.getName());

  private final EventHandler<ReefServiceProtos.JobStatusProto> jobStatusHandler;

  private ReefServiceProtos.JobStatusProto lastStatus = null;

  @Inject
  private RemoteClientJobStatusHandler(
      final RemoteManager remoteManager,
      @Parameter(ClientRemoteIdentifier.class) final String clientRID) {

    if (clientRID.equals(ClientRemoteIdentifier.NONE)) {
      LOG.log(Level.FINE, "Instantiated 'RemoteClientJobStatusHandler' without an actual connection to the client.");
      this.jobStatusHandler = new LoggingJobStatusHandler();
    } else {
      this.jobStatusHandler = remoteManager.getHandler(clientRID, ReefServiceProtos.JobStatusProto.class);
      LOG.log(Level.FINE, "Instantiated 'RemoteClientJobStatusHandler' for {0}", clientRID);
    }
  }

  @Override
  public void onNext(final ReefServiceProtos.JobStatusProto jobStatus) {
    this.lastStatus = jobStatus;
    this.jobStatusHandler.onNext(jobStatus);
  }

  /**
   * Return the last known status of the REEF job.
   * Can return null if the job has not been launched yet.
   * @return Last status of the REEF job.
   */
  @Override
  public ReefServiceProtos.JobStatusProto getLastStatus() {
    return this.lastStatus;
  }
}
