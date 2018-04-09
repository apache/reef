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
import org.apache.reef.bridge.client.events.*;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.JVMProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.exception.EvaluatorException;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorDescriptorImpl;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final JVMProcessFactory clientProcessFactory;

  private final TcpPortProvider tcpPortProvider;

  private final DriverClientDispatcher clientDriverDispatcher;

  private final Map<String, AllocatedEvaluatorBridge> evaluatorBridgeMap = new HashMap<>();

  private final Map<String, ActiveContextBridge> activeContextBridgeMap = new HashMap<>();

  @Inject
  private DriverClientService(
      final DriverServiceClient driverServiceClient,
      final JVMProcessFactory clientProcessFactory,
      final TcpPortProvider tcpPortProvider,
      final DriverClientDispatcher clientDriverDispatcher) {
    this.driverServiceClient = driverServiceClient;
    this.clientProcessFactory = clientProcessFactory;
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
    try {
      final StartTime startTime = new StartTime(request.getStartTime());
      this.clientDriverDispatcher.dispatch(startTime);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void stopHandler(final StopTimeInfo request, final StreamObserver<Void> responseObserver) {
    try {
      final StopTime stopTime = new StopTime(request.getStopTime());
      this.clientDriverDispatcher.dispatch(stopTime);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void alarmTrigger(final AlarmTriggerInfo request, final StreamObserver<Void> responseObserver) {
    try {
      this.clientDriverDispatcher.dispatchAlarm(request.getAlarmId());
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void allocatedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try {
      final AllocatedEvaluatorBridge eval = new AllocatedEvaluatorBridge(
          request.getEvaluatorId(),
          toEvaluatorDescriptor(request.getDescriptorInfo()),
          this.driverServiceClient);
      this.evaluatorBridgeMap.put(eval.getId(), eval);
      this.clientDriverDispatcher.dispatch(eval);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void completedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try {
      this.evaluatorBridgeMap.remove(request.getEvaluatorId());
      this.clientDriverDispatcher.dispatch(new CompletedEvaluatorBridge(request.getEvaluatorId()));
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void failedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try {
      final AllocatedEvaluatorBridge eval = this.evaluatorBridgeMap.remove(request.getEvaluatorId());
      List<FailedContext> failedContextList = new ArrayList<>();
      if (request.getFailure().getFailedContextsList() != null) {
        for (final String failedContextId : request.getFailure().getFailedContextsList()) {
          final ActiveContextBridge context = this.activeContextBridgeMap.get(failedContextId);
          failedContextList.add(new FailedContextBridge(
              context.getId(),
              eval.getId(),
              request.getFailure().getMessage(),
              eval.getEvaluatorDescriptor(),
              context.getParentId().isPresent() ?
                  Optional.<ActiveContext>of(this.activeContextBridgeMap.get(context.getParentId().get())) :
                  Optional.<ActiveContext>empty(),
              Optional.<byte[]>empty()));
        }
        for (final String failedContextId : request.getFailure().getFailedContextsList()) {
          this.activeContextBridgeMap.remove(failedContextId);
        }
      }
      this.clientDriverDispatcher.dispatch(
          new FailedEvaluatorBridge(
              eval.getId(),
              new EvaluatorException(request.getEvaluatorId(), request.getFailure().getMessage()),
              failedContextList,
              request.getFailure().getFailedTaskId() != null ?
                  Optional.of(new FailedTask(
                      request.getFailure().getFailedTaskId(),
                      request.getFailure().getMessage(),
                      Optional.<String>empty(),
                      Optional.<Throwable>empty(),
                      Optional.<byte[]>empty(),
                      Optional.<ActiveContext>empty())) :
                  Optional.<FailedTask>empty()));
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void activeContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    try {
      final AllocatedEvaluatorBridge eval = this.evaluatorBridgeMap.get(request.getEvaluatorId());
      final ActiveContextBridge context = new ActiveContextBridge(
          this.driverServiceClient,
          request.getContextId(),
          request.getParentId() != null ? Optional.of(request.getParentId()) : Optional.<String>empty(),
          eval.getId(),
          eval.getEvaluatorDescriptor());
      this.activeContextBridgeMap.put(context.getId(), context);
      this.clientDriverDispatcher.dispatch(context);
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void closedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        final ActiveContextBridge context = this.activeContextBridgeMap.remove(request.getContextId());
        this.clientDriverDispatcher.dispatch(
            new ClosedContextBridge(
                context.getId(),
                context.getEvaluatorId(),
                this.activeContextBridgeMap.get(request.getParentId()),
                context.getEvaluatorDescriptor()));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void failedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        final ActiveContextBridge context = this.activeContextBridgeMap.remove(request.getContextId());
        final Optional<ActiveContext> parent = context.getParentId().isPresent() ?
            Optional.<ActiveContext>of(this.activeContextBridgeMap.get(context.getParentId().get())) :
            Optional.<ActiveContext>empty();
        final Optional<byte[]> data = request.getException().getData() != null ?
            Optional.of(request.getException().getData().toByteArray()) : Optional.<byte[]>empty();
        this.clientDriverDispatcher.dispatch(
            new FailedContextBridge(
                context.getId(),
                context.getEvaluatorId(),
                request.getException().getMessage(),
                context.getEvaluatorDescriptor(),
                parent,
                data));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void contextMessageHandler(final ContextMessageInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        this.clientDriverDispatcher.dispatch(
            new ContextMessageBridge(
                request.getContextId(),
                request.getMessageSourceId(),
                request.getSequenceNumber(),
                request.getPayload().toByteArray()));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void runningTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        final ActiveContextBridge context = this.activeContextBridgeMap.get(request.getContextId());
        this.clientDriverDispatcher.dispatch(
            new RunningTaskBridge(this.driverServiceClient, request.getTaskId(), context));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onError(new IllegalArgumentException("Unknown context id: " + request.getContextId()));
    }
  }

  @Override
  public void failedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    try {
      final Optional<ActiveContext> context = this.activeContextBridgeMap.containsKey(request.getContextId()) ?
          Optional.<ActiveContext>of(this.activeContextBridgeMap.get(request.getContextId())) :
          Optional.<ActiveContext>empty();
      final Optional<byte[]> data = request.getException().getData() != null ?
          Optional.of(request.getException().getData().toByteArray()) : Optional.<byte[]>empty();
      this.clientDriverDispatcher.dispatch(
          new FailedTask(
              request.getTaskId(),
              request.getException().getMessage(),
              Optional.of(request.getException().getName()),
              Optional.<Throwable>of(new EvaluatorException(request.getException().getMessage())),
              data,
              context));
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void completedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        final ActiveContextBridge context = this.activeContextBridgeMap.get(request.getContextId());
        this.clientDriverDispatcher.dispatch(
            new CompletedTaskBridge(
                request.getTaskId(),
                context,
                request.getResult() != null ? request.getResult().toByteArray() : null));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onError(new IllegalArgumentException("Unknown context id: " + request.getContextId()));
    }
  }

  @Override
  public void suspendedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    super.suspendedTaskHandler(request, responseObserver);
  }

  @Override
  public void taskMessageHandler(final TaskMessageInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      try {
        this.clientDriverDispatcher.dispatch(
            new TaskMessageBridge(
                request.getTaskId(),
                request.getContextId(),
                request.getMessageSourceId(),
                request.getSequenceNumber(),
                request.getPayload().toByteArray()));
      } finally {
        responseObserver.onCompleted();
      }
    } else {
      responseObserver.onError(new IllegalArgumentException("Unknown context id: " + request.getContextId()));
    }
  }

  @Override
  public void clientMessageHandler(final ClientMessageInfo request, final StreamObserver<Void> responseObserver) {
    try {
      this.clientDriverDispatcher.clientMessageDispatch(request.getPayload().toByteArray());
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void clientCloseHandler(final Void request, final StreamObserver<Void> responseObserver) {
    try {
      this.clientDriverDispatcher.clientCloseDispatch();
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void clientCloseWithMessageHandler(
      final ClientMessageInfo request,
      final StreamObserver<Void> responseObserver) {
    try {
      this.clientDriverDispatcher.clientCloseWithMessageDispatch(request.getPayload().toByteArray());
    } finally {
      responseObserver.onCompleted();
    }
  }

  // Helper methods

  private EvaluatorDescriptor toEvaluatorDescriptor(final EvaluatorDescriptorInfo info) {
    final JVMProcess process = clientProcessFactory.newEvaluatorProcess();
    return new EvaluatorDescriptorImpl(
        null, info.getMemory(), info.getCores(), process, info.getRuntimeName());
  }
}
