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

package org.apache.reef.bridge.client.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.reef.bridge.client.DriverClientDispatcher;
import org.apache.reef.bridge.client.IDriverClientService;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver client service that accepts incoming messages driver service and
 * dispatches appropriate objects to the application.
 */
public final class DriverClientService extends DriverClientGrpc.DriverClientImplBase
    implements IDriverClientService {

  private static final Logger LOG = Logger.getLogger(DriverClientService.class.getName());

  private Server server;

  private final DriverServiceClient driverServiceClient;

  private final TcpPortProvider tcpPortProvider;

  private final DriverClientDispatcher clientDriverDispatcher;

  @Inject
  private DriverClientService(
      final DriverServiceClient driverServiceClient,
      final TcpPortProvider tcpPortProvider,
      final DriverClientDispatcher clientDriverDispatcher) {
    this.driverServiceClient = driverServiceClient;
    this.tcpPortProvider = tcpPortProvider;
    this.clientDriverDispatcher = clientDriverDispatcher;
  }

  @Override
  public void start() throws IOException {
    for (final Integer port : this.tcpPortProvider) {
      try {
        this.server = ServerBuilder.forPort(port)
            .addService(this)
            .build()
            .start();
        LOG.info("Driver Client Server started, listening on " + port);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Unable to bind to port [{0}]", port);
      }
    }
    if (this.server == null || this.server.isTerminated()) {
      throw new IOException("Unable to start gRPC server");
    }
    this.driverServiceClient.registerDriverClientService("localhost", this.server.getPort());
  }

  @Override
  public void startHandler(final StartTimeInfo request, final StreamObserver<Void> responseObserver) {
    final StartTime startTime = new StartTime(request.getStartTime());
    this.clientDriverDispatcher.dispatch(startTime);
  }

  @Override
  public void stopHandler(final StopTimeInfo request, final StreamObserver<Void> responseObserver) {
    final StopTime stopTime = new StopTime(request.getStopTime());
    this.clientDriverDispatcher.dispatch(stopTime);
  }

  @Override
  public void alarmTrigger(final AlarmTriggerInfo request, final StreamObserver<Void> responseObserver) {
    this.clientDriverDispatcher.dispatchAlarm(request.getAlarmId());
  }

  @Override
  public void allocatedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
  }

  @Override
  public void completedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    super.completedEvaluatorHandler(request, responseObserver);
  }

  @Override
  public void failedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    super.failedEvaluatorHandler(request, responseObserver);
  }

  @Override
  public void activeContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    super.activeContextHandler(request, responseObserver);
  }

  @Override
  public void closedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    super.closedContextHandler(request, responseObserver);
  }

  @Override
  public void failedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    super.failedContextHandler(request, responseObserver);
  }

  @Override
  public void contextMessageHandler(final ContextMessageInfo request, final StreamObserver<Void> responseObserver) {
    super.contextMessageHandler(request, responseObserver);
  }

  @Override
  public void runningTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    super.runningTaskHandler(request, responseObserver);
  }

  @Override
  public void failedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    super.failedTaskHandler(request, responseObserver);
  }

  @Override
  public void completedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    super.completedTaskHandler(request, responseObserver);
  }

  @Override
  public void suspendedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    super.suspendedTaskHandler(request, responseObserver);
  }

  @Override
  public void taskMessageHandler(final TaskMessageInfo request, final StreamObserver<Void> responseObserver) {
    super.taskMessageHandler(request, responseObserver);
  }

  @Override
  public void clientMessageHandler(final ClientMessageInfo request, final StreamObserver<Void> responseObserver) {
    super.clientMessageHandler(request, responseObserver);
  }

  @Override
  public void clientCloseHandler(final Void request, final StreamObserver<Void> responseObserver) {
    super.clientCloseHandler(request, responseObserver);
  }

  @Override
  public void clientCloseWithMessageHandler(
      final ClientMessageInfo request,
      final StreamObserver<Void> responseObserver) {
    super.clientCloseWithMessageHandler(request, responseObserver);
  }
}
