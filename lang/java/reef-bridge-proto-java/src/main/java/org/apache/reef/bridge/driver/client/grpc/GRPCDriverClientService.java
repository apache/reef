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

package org.apache.reef.bridge.driver.client.grpc;

import com.google.common.collect.Lists;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.DriverClientDispatcher;
import org.apache.reef.bridge.driver.client.DriverClientService;
import org.apache.reef.bridge.driver.client.JVMClientProcess;
import org.apache.reef.bridge.driver.client.events.*;
import org.apache.reef.bridge.driver.common.grpc.GRPCUtils;
import org.apache.reef.bridge.driver.common.grpc.ObserverCleanup;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.exception.EvaluatorException;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorDescriptorBuilderFactory;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver client service that accepts incoming messages driver service and
 * dispatches appropriate objects to the application.
 */
@Private
public final class GRPCDriverClientService extends DriverClientGrpc.DriverClientImplBase
    implements DriverClientService {

  private static final Logger LOG = Logger.getLogger(GRPCDriverClientService.class.getName());

  private Server server;

  private final Object lock = new Object();

  private final InjectionFuture<Clock> clock;

  private final ExceptionCodec exceptionCodec;

  private final GRPCDriverServiceClient driverServiceClient;

  private final TcpPortProvider tcpPortProvider;

  private final EvaluatorDescriptorBuilderFactory evaluatorDescriptorBuilderFactory;

  private final InjectionFuture<DriverClientDispatcher> clientDriverDispatcher;

  private final Map<String, AllocatedEvaluatorBridge> evaluatorBridgeMap = new HashMap<>();

  private final Map<String, ActiveContextBridge> activeContextBridgeMap = new HashMap<>();

  private int outstandingEvaluatorCount = 0;

  @Inject
  private GRPCDriverClientService(
      final EvaluatorDescriptorBuilderFactory evaluatorDescriptorBuilderFactory,
      final ExceptionCodec exceptionCodec,
      final GRPCDriverServiceClient driverServiceClient,
      final TcpPortProvider tcpPortProvider,
      final InjectionFuture<Clock> clock,
      final InjectionFuture<DriverClientDispatcher> clientDriverDispatcher) {
    this.evaluatorDescriptorBuilderFactory = evaluatorDescriptorBuilderFactory;
    this.exceptionCodec = exceptionCodec;
    this.driverServiceClient = driverServiceClient;
    this.tcpPortProvider = tcpPortProvider;
    this.clock = clock;
    this.clientDriverDispatcher = clientDriverDispatcher;
  }

  @Override
  public void notifyEvaluatorRequest(final int count) {
    synchronized (this.lock) {
      this.outstandingEvaluatorCount += count;
      this.lock.notify();
    }
  }

  @Override
  public void start() throws IOException {
    for (final int port : this.tcpPortProvider) {
      try {
        this.server = ServerBuilder.forPort(port)
            .addService(this)
            .build()
            .start();
        LOG.log(Level.INFO, "Driver Client Server started, listening on [{0}]", port);
        break;
      } catch (final IOException e) {
        LOG.log(Level.WARNING, "Unable to bind to port [{0}]", port);
      }
    }
    if (this.server == null || this.server.isTerminated()) {
      throw new IOException("Unable to start gRPC server");
    }
    this.driverServiceClient.registerDriverClientService("localhost", this.server.getPort());
  }

  @Override
  public void awaitTermination() throws InterruptedException {
    if (this.server != null) {
      this.server.awaitTermination();
    }
  }

  @Override
  public void idlenessCheckHandler(final Void request, final StreamObserver<IdleStatus> responseObserver) {
    if (isIdle()) {
      LOG.log(Level.INFO, "possibly idle. waiting for some action.");
      try {
        synchronized (this.lock) {
          this.lock.wait(1000); // wait a second
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.FINEST, "Idleness checker wait interrupted");
      }
    }
    responseObserver.onNext(IdleStatus.newBuilder()
        .setReason("DriverClient checking idleness")
        .setIsIdle(this.isIdle())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void startHandler(final StartTimeInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "StartHandler at time {0}", request.getStartTime());
      final StartTime startTime = new StartTime(request.getStartTime());
      this.clientDriverDispatcher.get().dispatch(startTime);
    }
  }

  @Override
  public void stopHandler(final StopTimeInfo request, final StreamObserver<ExceptionInfo> responseObserver) {
    try {
      LOG.log(Level.INFO, "StopHandler at time {0}", request.getStopTime());
      final StopTime stopTime = new StopTime(request.getStopTime());
      final Throwable error = this.clientDriverDispatcher.get().dispatch(stopTime);
      if (error != null) {
        responseObserver.onNext(GRPCUtils.createExceptionInfo(this.exceptionCodec, error));
      } else {
        responseObserver.onNext(ExceptionInfo.newBuilder().setNoError(true).build());
      }
    } finally {
      responseObserver.onCompleted();
      this.server.shutdown();
    }
  }

  @Override
  public void alarmTrigger(final AlarmTriggerInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Alarm Trigger id {0}", request.getAlarmId());
      this.clientDriverDispatcher.get().dispatchAlarm(request.getAlarmId());
    }
  }

  @Override
  public void allocatedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      synchronized (this.lock) {
        assert this.outstandingEvaluatorCount > 0;
        this.outstandingEvaluatorCount--;
      }
      LOG.log(Level.INFO, "Allocated evaluator id {0}", request.getEvaluatorId());
      final AllocatedEvaluatorBridge eval = new AllocatedEvaluatorBridge(
          request.getEvaluatorId(),
          toEvaluatorDescriptor(request.getDescriptorInfo()),
          this.driverServiceClient);
      this.evaluatorBridgeMap.put(eval.getId(), eval);
      this.clientDriverDispatcher.get().dispatch(eval);
    }
  }

  @Override
  public void completedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Completed Evaluator id {0}", request.getEvaluatorId());
      this.evaluatorBridgeMap.remove(request.getEvaluatorId());
      this.clientDriverDispatcher.get().dispatch(new CompletedEvaluatorBridge(request.getEvaluatorId()));
    }
  }

  @Override
  public void failedEvaluatorHandler(final EvaluatorInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      if (!this.evaluatorBridgeMap.containsKey(request.getEvaluatorId())) {
        LOG.log(Level.INFO, "Failed evalautor that we were not allocated");
        synchronized (this.lock) {
          if (this.outstandingEvaluatorCount > 0) {
            this.outstandingEvaluatorCount--;
          }
        }
        return;
      }
      LOG.log(Level.INFO, "Failed Evaluator id {0}", request.getEvaluatorId());
      final AllocatedEvaluatorBridge eval = this.evaluatorBridgeMap.remove(request.getEvaluatorId());
      final List<FailedContext> failedContextList = new ArrayList<>();
      if (request.getFailure().getFailedContextsList() != null) {
        for (final String failedContextId : request.getFailure().getFailedContextsList()) {
          final ActiveContextBridge context = this.activeContextBridgeMap.get(failedContextId);
          failedContextList.add(new FailedContextBridge(
              context.getId(),
              eval.getId(),
              request.getFailure().getMessage(),
              eval.getEvaluatorDescriptor(),
              Optional.<ActiveContext>ofNullable(this.activeContextBridgeMap.get(context.getParentId().get())),
              Optional.<Throwable>empty()));
        }
        for (final String failedContextId : request.getFailure().getFailedContextsList()) {
          this.activeContextBridgeMap.remove(failedContextId);
        }
      }
      this.clientDriverDispatcher.get().dispatch(
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
    }
  }

  @Override
  public void activeContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Active context id {0}", request.getContextId());
      final AllocatedEvaluatorBridge eval = this.evaluatorBridgeMap.get(request.getEvaluatorId());
      final ActiveContextBridge context = new ActiveContextBridge(
          this.driverServiceClient,
          request.getContextId(),
          Optional.ofNullable(request.getParentId()),
          eval.getId(),
          eval.getEvaluatorDescriptor());
      this.activeContextBridgeMap.put(context.getId(), context);
      this.clientDriverDispatcher.get().dispatch(context);
    }
  }

  @Override
  public void closedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      LOG.log(Level.INFO, "Closed context id {0}", request.getContextId());
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        final ActiveContextBridge context = this.activeContextBridgeMap.remove(request.getContextId());
        this.clientDriverDispatcher.get().dispatch(
            new ClosedContextBridge(
                context.getId(),
                context.getEvaluatorId(),
                this.activeContextBridgeMap.get(request.getParentId()),
                context.getEvaluatorDescriptor()));
      }
    } else {
      responseObserver.onError(Status.INTERNAL
          .withDescription("Unknown context id " + request.getContextId() + " in close")
          .asRuntimeException());
    }
  }

  @Override
  public void failedContextHandler(final ContextInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      LOG.log(Level.INFO, "Failed context id {0}", request.getContextId());
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        final ActiveContextBridge context = this.activeContextBridgeMap.remove(request.getContextId());
        final Optional<ActiveContext> parent =
            Optional.<ActiveContext>ofNullable(this.activeContextBridgeMap.get(context.getParentId().get()));
        final Optional<Throwable> reason =
            this.exceptionCodec.fromBytes(request.getException().getData().toByteArray());
        this.clientDriverDispatcher.get().dispatch(
            new FailedContextBridge(
                context.getId(),
                context.getEvaluatorId(),
                request.getException().getMessage(),
                context.getEvaluatorDescriptor(),
                parent,
                reason));
      }
    } else {
      responseObserver.onError(Status.INTERNAL
          .withDescription("Unknown context id " + request.getContextId() + " in close")
          .asRuntimeException());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void contextMessageHandler(final ContextMessageInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      LOG.log(Level.INFO, "Message context id {0}", request.getContextId());
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        this.clientDriverDispatcher.get().dispatch(
            new ContextMessageBridge(
                request.getContextId(),
                request.getMessageSourceId(),
                request.getSequenceNumber(),
                request.getPayload().toByteArray()));
      }
    } else {
      responseObserver.onError(Status.INTERNAL
          .withDescription("Unknown context id " + request.getContextId() + " in close")
          .asRuntimeException());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void runningTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Running task id {0}", request.getTaskId());
      final ContextInfo contextInfo = request.getContext();
      final ActiveContextBridge context = addContextIfMissing(contextInfo);
      this.clientDriverDispatcher.get().dispatch(
          new RunningTaskBridge(this.driverServiceClient, request.getTaskId(), context));
    }
  }

  @Override
  public void failedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Failed task id {0}", request.getTaskId());
      ActiveContextBridge context = request.hasContext() ?
          addContextIfMissing(request.getContext()) : null;
      this.clientDriverDispatcher.get().dispatch(
          new FailedTask(
              request.getTaskId(),
              request.getException().getMessage(),
              Optional.of(request.getException().getName()),
              request.getException().getData().isEmpty() ?
                  Optional.<Throwable>of(new EvaluatorException(request.getException().getMessage())) :
                  this.exceptionCodec.fromBytes(request.getException().getData().toByteArray()),
              Optional.<byte[]>empty(),
              Optional.<ActiveContext>ofNullable(context)));
    }
  }

  @Override
  public void completedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Completed task id {0}", request.getTaskId());
      final ContextInfo contextInfo = request.getContext();
      ActiveContextBridge context = addContextIfMissing(contextInfo);
      this.clientDriverDispatcher.get().dispatch(
          new CompletedTaskBridge(
              request.getTaskId(),
              context,
              GRPCUtils.toByteArray(request.getResult())));
    }
  }

  @Override
  public void suspendedTaskHandler(final TaskInfo request, final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Suspended task id {0}", request.getTaskId());
      final ContextInfo contextInfo = request.getContext();
      ActiveContextBridge context = addContextIfMissing(contextInfo);
      this.clientDriverDispatcher.get().dispatch(
          new SuspendedTaskBridge(
              request.getTaskId(),
              context,
              GRPCUtils.toByteArray(request.getResult())));
    }
  }

  @Override
  public void taskMessageHandler(final TaskMessageInfo request, final StreamObserver<Void> responseObserver) {
    if (this.activeContextBridgeMap.containsKey(request.getContextId())) {
      LOG.log(Level.INFO, "Message task id {0}", request.getTaskId());
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        this.clientDriverDispatcher.get().dispatch(
            new TaskMessageBridge(
                request.getTaskId(),
                request.getContextId(),
                request.getMessageSourceId(),
                request.getSequenceNumber(),
                request.getPayload().toByteArray()));
      }
    } else {
      responseObserver.onError(Status.INTERNAL
          .withDescription("Unknown context id: " + request.getContextId())
          .asRuntimeException());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void clientMessageHandler(final ClientMessageInfo request, final StreamObserver<Void> responseObserver) {
    LOG.log(Level.INFO, "Client message");
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      this.clientDriverDispatcher.get().clientMessageDispatch(request.getPayload().toByteArray());
    }
  }

  @Override
  public void clientCloseHandler(final Void request, final StreamObserver<Void> responseObserver) {
    LOG.log(Level.INFO, "Client close");
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      this.clientDriverDispatcher.get().clientCloseDispatch();
    }
  }

  @Override
  public void clientCloseWithMessageHandler(
      final ClientMessageInfo request,
      final StreamObserver<Void> responseObserver) {
    LOG.log(Level.INFO, "Client close with message");
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      this.clientDriverDispatcher.get().clientCloseWithMessageDispatch(request.getPayload().toByteArray());
    }
  }

  @Override
  public void driverRestartHandler(final DriverRestartInfo request, final StreamObserver<Void> responseObserver) {
    LOG.log(Level.INFO, "Driver restarted");
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      final DriverRestarted driverRestarted = new DriverRestarted() {
        @Override
        public int getResubmissionAttempts() {
          return request.getResubmissionAttempts();
        }

        @Override
        public StartTime getStartTime() {
          return new StartTime(request.getStartTime().getStartTime());
        }

        @Override
        public Set<String> getExpectedEvaluatorIds() {
          return new HashSet<>(request.getExpectedEvaluatorIdsList());
        }
      };
      this.clientDriverDispatcher.get().dispatchRestart(driverRestarted);
    }
  }

  @Override
  public void driverRestartActiveContextHandler(
      final ContextInfo request,
      final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Driver restarted active context {0}", request.getContextId());
      if (!this.evaluatorBridgeMap.containsKey(request.getEvaluatorId())) {
        final AllocatedEvaluatorBridge eval = new AllocatedEvaluatorBridge(
            request.getEvaluatorId(),
            toEvaluatorDescriptor(request.getEvaluatorDescriptorInfo()),
            this.driverServiceClient);
        this.evaluatorBridgeMap.put(eval.getId(), eval);
      }
      final ActiveContextBridge context = addContextIfMissing(request);
      this.clientDriverDispatcher.get().dispatchRestart(context);
    }
  }

  @Override
  public void driverRestartRunningTaskHandler(
      final TaskInfo request,
      final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      LOG.log(Level.INFO, "Driver restarted running task {0}", request.getTaskId());
      if (!this.evaluatorBridgeMap.containsKey(request.getContext().getEvaluatorId())) {
        final AllocatedEvaluatorBridge eval = new AllocatedEvaluatorBridge(
            request.getContext().getEvaluatorId(),
            toEvaluatorDescriptor(request.getContext().getEvaluatorDescriptorInfo()),
            this.driverServiceClient);
        this.evaluatorBridgeMap.put(eval.getId(), eval);
      }
      final ActiveContextBridge context = addContextIfMissing(request.getContext());
      this.clientDriverDispatcher.get().dispatchRestart(
          new RunningTaskBridge(this.driverServiceClient, request.getTaskId(), context));
    }
  }

  @Override
  public void driverRestartCompletedHandler(
      final DriverRestartCompletedInfo request,
      final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      this.clientDriverDispatcher.get().dispatchRestart(new DriverRestartCompleted() {
        @Override
        public Time getCompletedTime() {
          return new StopTime(request.getCompletionTime().getStopTime());
        }

        @Override
        public boolean isTimedOut() {
          return request.getIsTimedOut();
        }
      });
    }
  }

  @Override
  public void driverRestartFailedEvaluatorHandler(
      final EvaluatorInfo request,
      final StreamObserver<Void> responseObserver) {
    try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
      this.clientDriverDispatcher.get().dispatchRestart(new FailedEvaluatorBridge(
          request.getEvaluatorId(),
          new EvaluatorException(request.getFailure() != null ?
              request.getFailure().getMessage() : "restart failed"),
          Lists.<FailedContext>newArrayList(),
          Optional.<FailedTask>empty()));
    }
  }

  // Helper methods
  private boolean isIdle() {
    LOG.log(Level.INFO, "Clock idle {0}, outstanding evaluators {1}, current evaluators {2}",
        new Object[] {
            this.clock.get().isIdle(),
            this.outstandingEvaluatorCount,
            this.evaluatorBridgeMap.isEmpty()});
    return clock.get().isIdle() &&
        this.outstandingEvaluatorCount == 0 &&
        this.evaluatorBridgeMap.isEmpty();
  }

  private ActiveContextBridge addContextIfMissing(final ContextInfo contextInfo) {
    final String contextId = contextInfo.getContextId();
    ActiveContextBridge context = this.activeContextBridgeMap.get(contextId);
    if (context == null) {
      context = toActiveContext(contextInfo);
      this.activeContextBridgeMap.put(contextId, context);
    }
    return context;
  }

  private EvaluatorDescriptor toEvaluatorDescriptor(final EvaluatorDescriptorInfo info) {
    final NodeDescriptor nodeDescriptor = new NodeDescriptor() {
      @Override
      public InetSocketAddress getInetSocketAddress() {
        return InetSocketAddress.createUnresolved(
            info.getNodeDescriptorInfo().getIpAddress(),
            info.getNodeDescriptorInfo().getPort());
      }

      @Override
      public RackDescriptor getRackDescriptor() {
        return new RackDescriptor() {
          @Override
          public List<NodeDescriptor> getNodes() {
            return Lists.newArrayList();
          }

          @Override
          public String getName() {
            return info.getNodeDescriptorInfo().getRackName();
          }
        };
      }

      @Override
      public String getName() {
        return info.getNodeDescriptorInfo().getHostName();
      }

      @Override
      public String getId() {
        return info.getNodeDescriptorInfo().getId();
      }
    };
    return this.evaluatorDescriptorBuilderFactory.newBuilder()
        .setNodeDescriptor(nodeDescriptor)
        .setMemory(info.getMemory())
        .setNumberOfCores(info.getCores())
        .setEvaluatorProcess(new JVMClientProcess())
        .setRuntimeName(info.getRuntimeName())
        .build();
  }

  private ActiveContextBridge toActiveContext(final ContextInfo contextInfo) {
    final AllocatedEvaluatorBridge eval = this.evaluatorBridgeMap.get(contextInfo.getEvaluatorId());
    return new ActiveContextBridge(
        this.driverServiceClient,
        contextInfo.getContextId(),
        StringUtils.isNotEmpty(contextInfo.getParentId()) ?
            Optional.of(contextInfo.getParentId()) : Optional.<String>empty(),
        eval.getId(),
        eval.getEvaluatorDescriptor());
  }
}
