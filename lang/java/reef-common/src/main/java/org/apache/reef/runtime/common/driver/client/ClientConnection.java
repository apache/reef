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

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the communication channel to the client.
 */
@DriverSide
public final class ClientConnection {

  private static final Logger LOG = Logger.getLogger(ClientConnection.class.getName());

  private final JobStatusHandler jobStatusHandler;
  private final String jobIdentifier;

  @Inject
  public ClientConnection(
      @Parameter(JobIdentifier.class) final String jobIdentifier,
      final JobStatusHandler jobStatusHandler) {

    this.jobIdentifier = jobIdentifier;
    this.jobStatusHandler = jobStatusHandler;
  }

  /**
   * Send the given JobStatus to the client.
   *
   * @param status
   */
  public synchronized void send(final ReefServiceProtos.JobStatusProto status) {
    LOG.log(Level.FINEST, "Sending:\n" + status);
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

