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
package org.apache.reef.bridge.driver.service.grpc;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.reef.bridge.driver.service.DriverClientException;
import org.apache.reef.bridge.driver.service.IDriverService;
import org.apache.reef.bridge.service.parameters.DriverClientCommand;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.AllocatedEvaluatorImpl;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.OSUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GRPC DriverBridgeService that interacts with higher-level languages.
 */
public final class GRPCDriverService implements IDriverService {
  private static final Logger LOG = Logger.getLogger(GRPCDriverService.class.getName());

  private static final Void VOID = Void.newBuilder().build();

  private Server server;

  private Process driverProcess;

  private DriverClientGrpc.DriverClientFutureStub clientStub;

  private final Clock clock;

  private final ConfigurationSerializer configurationSerializer;

  private final EvaluatorRequestor evaluatorRequestor;

  private final JVMProcessFactory jvmProcessFactory;

  private final CLRProcessFactory clrProcessFactory;

  private final TcpPortProvider tcpPortProvider;

  private final String driverClientCommand;

  private final Map<String, AllocatedEvaluator> allocatedEvaluatorMap = new HashMap<>();

  private final Map<String, ActiveContext> activeContextMap = new HashMap<>();

  private final Map<String, RunningTask> runningTaskMap = new HashMap<>();

  private boolean stopped = false;

  @Inject
  private GRPCDriverService(
      final Clock clock,
      final EvaluatorRequestor evaluatorRequestor,
      final ConfigurationSerializer configurationSerializer,
      final JVMProcessFactory jvmProcessFactory,
      final CLRProcessFactory clrProcessFactory,
      final TcpPortProvider tcpPortProvider,
      @Parameter(DriverClientCommand.class) final String driverClientCommand) {
    this.clock = clock;
    this.configurationSerializer = configurationSerializer;
    this.jvmProcessFactory = jvmProcessFactory;
    this.clrProcessFactory = clrProcessFactory;
    this.evaluatorRequestor = evaluatorRequestor;
    this.driverClientCommand = driverClientCommand;
    this.tcpPortProvider = tcpPortProvider;
  }

  private void start() throws IOException, InterruptedException {
    for (final Integer port : this.tcpPortProvider) {
      try {
        this.server = ServerBuilder.forPort(port)
            .addService(new DriverBridgeServiceImpl())
            .build()
            .start();
        LOG.info("Server started, listening on " + port);
        break;
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Unable to bind to port [{0}]", port);
      }
    }
    if (this.server == null || this.server.isTerminated()) {
      throw new IOException("Unable to start gRPC server");
    } else {
      final String cmd = this.driverClientCommand + " " + this.server.getPort();
      final String cmdOs = OSUtils.isWindows() ? "cmd.exe /c \"" + cmd + "\"" : cmd;
      LOG.log(Level.INFO, "CMD: " + cmdOs);
      this.driverProcess = Runtime.getRuntime().exec(cmdOs);
      synchronized (this) {
        // wait for driver client process to register
        while (this.clientStub == null && driverProcessIsAlive()) {
          LOG.log(Level.INFO, "waiting for driver process to register");
          this.wait(1000); // a second
        }
      }
      if (driverProcessIsAlive()) {
        Thread closeChildThread = new Thread() {
          public void run() {
            synchronized (GRPCDriverService.this) {
              if (GRPCDriverService.this.driverProcess != null) {
                GRPCDriverService.this.driverProcess.destroy();
                GRPCDriverService.this.driverProcess = null;
              }
            }
          }
        };
        Runtime.getRuntime().addShutdownHook(closeChildThread);
      }
    }
  }

  private void stop() {
    stop(null);
  }

  private void stop(final Throwable t) {
    LOG.log(Level.INFO, "STOP: gRPC Driver Service", t);
    if (!stopped) {
      try {
        if (t != null) {
          clock.stop(t);
        } else {
          clock.stop();
        }
        if (server != null) {
          LOG.log(Level.INFO, "Shutdown gRPC");
          this.server.shutdown();
          this.server = null;
        }
        if (this.driverProcess != null) {
          LOG.log(Level.INFO, "shutdown driver process");
          dump();
          this.driverProcess.destroy();
          this.driverProcess = null;
        }
      } finally {
        LOG.log(Level.INFO, "COMPLETED STOP: gRPC Driver Service");
        stopped = true;
      }
    }
  }

  private void dump() {
    if (!driverProcessIsAlive()) {
      LOG.log(Level.INFO, "Exit code: " + this.driverProcess.exitValue());
    }
    LOG.log(Level.INFO, "capturing driver process stderr");
    StringBuffer errBuffer = new StringBuffer();
    InputStream errStream = this.driverProcess.getErrorStream();
    try {
      int nextChar;
      errBuffer.append("\n==============================================\n");
      while ((nextChar = errStream.read()) != -1) {
        errBuffer.append((char) nextChar);
      }
      errBuffer.append("\n==============================================\n");
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Error while capturing output stream: " + e.getMessage());
    }
    LOG.log(Level.INFO, errBuffer.toString());
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Determines if the driver process is still alive by
   * testing for its exit value, which throws {@link IllegalThreadStateException}
   * if process is still running.
   * @return true if driver process is alive, false otherwise
   */
  private boolean driverProcessIsAlive() {
    if (this.driverProcess != null) {
      try {
        this.driverProcess.exitValue();
      } catch (IllegalThreadStateException e) {
        return true;
      }
    }
    return false;
  }

  private EvaluatorDescriptorInfo toEvaluatorDescriptorInfo(final EvaluatorDescriptor descriptor) {
    if (descriptor == null) {
      return null;
    } else {
      return EvaluatorDescriptorInfo.newBuilder()
          .setCores(descriptor.getNumberOfCores())
          .setMemory(descriptor.getMemory())
          .setRuntimeName(descriptor.getRuntimeName())
          .build();
    }
  }

  @Override
  public IdleMessage getIdleStatus() {
    final String componentName = "Java Bridge DriverService";
    if (this.clientStub != null) {
      try {
        final IdleStatus idleStatus = this.clientStub.idlenessCheckHandler(VOID).get();
        LOG.log(Level.INFO, "is idle: " + idleStatus.getIsIdle());
        return new IdleMessage(
            componentName,
            idleStatus.getReason(),
            idleStatus.getIsIdle());
      } catch (ExecutionException | InterruptedException e) {
        stop(e);
      }
    }
    return new IdleMessage(
        componentName,
        "stub not initialized",
        true);
  }

  @Override
  public void startHandler(final StartTime startTime) {
    try {
      start();
      synchronized (this) {
        if (this.clientStub != null) {
          this.clientStub.startHandler(
              StartTimeInfo.newBuilder().setStartTime(startTime.getTimestamp()).build());
        } else {
          stop(new IllegalStateException("Unable to start driver client"));
        }
      }
    } catch (IOException | InterruptedException e) {
      stop(e);
    }
  }

  @Override
  public void stopHandler(final StopTime stopTime) {
    synchronized (this) {
      try {
        if (clientStub != null) {
          this.clientStub.stopHandler(
              StopTimeInfo.newBuilder().setStopTime(stopTime.getTimestamp()).build());
        }
      } finally {
        stop();
      }
    }

  }

  @Override
  public void allocatedEvaluatorHandler(final AllocatedEvaluator eval) {
    synchronized (this) {
      this.allocatedEvaluatorMap.put(eval.getId(), eval);
      this.clientStub.allocatedEvaluatorHandler(
          EvaluatorInfo.newBuilder()
              .setEvaluatorId(eval.getId())
              .setDescriptorInfo(toEvaluatorDescriptorInfo(eval.getEvaluatorDescriptor()))
              .build());
    }
  }

  @Override
  public void completedEvaluatorHandler(final CompletedEvaluator eval) {
    synchronized (this) {
      this.allocatedEvaluatorMap.remove(eval.getId());
      this.clientStub.completedEvaluatorHandler(
          EvaluatorInfo.newBuilder().setEvaluatorId(eval.getId()).build());
    }
  }

  @Override
  public void failedEvaluatorHandler(final FailedEvaluator eval) {
    synchronized (this) {
      this.allocatedEvaluatorMap.remove(eval.getId());
      this.clientStub.failedEvaluatorHandler(
          EvaluatorInfo.newBuilder().setEvaluatorId(eval.getId()).build());
    }
  }

  @Override
  public void activeContextHandler(final ActiveContext context) {
    synchronized (this) {
      this.activeContextMap.put(context.getId(), context);
      this.clientStub.activeContextHandler(
          ContextInfo.newBuilder()
              .setContextId(context.getId())
              .setEvaluatorId(context.getEvaluatorId())
              .setParentId(
                  context.getParentId().isPresent() ?
                      context.getParentId().get() : null)
              .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                  context.getEvaluatorDescriptor()))
              .build());
    }
  }

  @Override
  public void closedContextHandler(final ClosedContext context) {
    synchronized (this) {
      this.activeContextMap.remove(context.getId());
      this.clientStub.closedContextHandler(
          ContextInfo.newBuilder()
              .setContextId(context.getId())
              .setEvaluatorId(context.getEvaluatorId())
              .setParentId(context.getParentContext().getId())
              .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                  context.getEvaluatorDescriptor()))
              .build());
    }
  }

  @Override
  public void failedContextHandler(final FailedContext context) {
    synchronized (this) {
      this.activeContextMap.remove(context.getId());
      this.clientStub.closedContextHandler(
          ContextInfo.newBuilder()
              .setContextId(context.getId())
              .setEvaluatorId(context.getEvaluatorId())
              .setParentId(
                  context.getParentContext().isPresent() ?
                      context.getParentContext().get().getId() : null)
              .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                  context.getEvaluatorDescriptor()))
              .build());
    }
  }

  @Override
  public void contextMessageHandler(final ContextMessage message) {
    synchronized (this) {
      this.clientStub.contextMessageHandler(
          ContextMessageInfo.newBuilder()
              .setContextId(message.getId())
              .setMessageSourceId(message.getMessageSourceID())
              .setSequenceNumber(message.getSequenceNumber())
              .setPayload(ByteString.copyFrom(message.get()))
              .build());
    }
  }

  @Override
  public void runningTaskHandler(final RunningTask task) {
    synchronized (this) {
      final ActiveContext context = task.getActiveContext();
      if (!this.activeContextMap.containsKey(context.getId())) {
        this.activeContextMap.put(context.getId(), context);
      }
      this.runningTaskMap.put(task.getId(), task);
      this.clientStub.runningTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContext(ContextInfo.newBuilder()
                  .setContextId(context.getId())
                  .setEvaluatorId(context.getEvaluatorId())
                  .setParentId(context.getParentId().isPresent() ? context.getParentId().get() : "")
                  .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                      task.getActiveContext().getEvaluatorDescriptor()))
                  .build())
              .build());
    }
  }

  @Override
  public void failedTaskHandler(final FailedTask task) {
    synchronized (this) {
      if (task.getActiveContext().isPresent() &&
          !this.activeContextMap.containsKey(task.getActiveContext().get().getId())) {
        this.activeContextMap.put(task.getActiveContext().get().getId(), task.getActiveContext().get());
      }
      this.runningTaskMap.remove(task.getId());
      this.clientStub.failedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContext(task.getActiveContext().isPresent() ?
                      ContextInfo.newBuilder()
                          .setContextId(task.getActiveContext().get().getId())
                          .setEvaluatorId(task.getActiveContext().get().getEvaluatorId())
                          .setParentId(task.getActiveContext().get().getParentId().isPresent() ?
                              task.getActiveContext().get().getParentId().get() : "")
                          .build() : null)
              .build());
    }
  }

  @Override
  public void completedTaskHandler(final CompletedTask task) {
    synchronized (this) {
      if (!this.activeContextMap.containsKey(task.getActiveContext().getId())) {
        this.activeContextMap.put(task.getActiveContext().getId(), task.getActiveContext());
      }
      this.runningTaskMap.remove(task.getId());
      this.clientStub.completedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContext(ContextInfo.newBuilder()
                  .setContextId(task.getActiveContext().getId())
                  .setEvaluatorId(task.getActiveContext().getEvaluatorId())
                  .setParentId(task.getActiveContext().getParentId().isPresent() ?
                      task.getActiveContext().getParentId().get() : "")
                  .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                      task.getActiveContext().getEvaluatorDescriptor()))
                  .build())
              .build());
    }
  }

  @Override
  public void suspendedTaskHandler(final SuspendedTask task) {
    synchronized (this) {
      if (!this.activeContextMap.containsKey(task.getActiveContext().getId())) {
        this.activeContextMap.put(task.getActiveContext().getId(), task.getActiveContext());
      }
      this.runningTaskMap.remove(task.getId());
      this.clientStub.suspendedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContext(ContextInfo.newBuilder()
                  .setContextId(task.getActiveContext().getId())
                  .setEvaluatorId(task.getActiveContext().getEvaluatorId())
                  .setParentId(task.getActiveContext().getParentId().isPresent() ?
                      task.getActiveContext().getParentId().get() : "")
                  .build())
              .build());
    }
  }

  @Override
  public void taskMessageHandler(final TaskMessage message) {
    synchronized (this) {
      this.clientStub.taskMessageHandler(
          TaskMessageInfo.newBuilder()
              .setTaskId(message.getId())
              .setContextId(message.getContextId())
              .setMessageSourceId(message.getMessageSourceID())
              .setSequenceNumber(message.getSequenceNumber())
              .setPayload(ByteString.copyFrom(message.get()))
              .build());
    }
  }

  @Override
  public void clientMessageHandler(final byte[] message) {
    synchronized (this) {
      this.clientStub.clientMessageHandler(
          ClientMessageInfo.newBuilder()
              .setPayload(ByteString.copyFrom(message))
              .build());
    }
  }

  @Override
  public void clientCloseHandler() {
    synchronized (this) {
      this.clientStub.clientCloseHandler(VOID);
    }
  }

  @Override
  public void clientCloseWithMessageHandler(final byte[] message) {
    synchronized (this) {
      this.clientStub.clientCloseWithMessageHandler(
          ClientMessageInfo.newBuilder()
              .setPayload(ByteString.copyFrom(message))
              .build());
    }
  }

  @Override
  public void driverRestarted(final DriverRestarted restart) {
    try {
      start();
      synchronized (this) {
        if (this.clientStub != null) {
          this.clientStub.driverRestartHandler(DriverRestartInfo.newBuilder()
              .setResubmissionAttempts(restart.getResubmissionAttempts())
              .setStartTime(StartTimeInfo.newBuilder()
                  .setStartTime(restart.getStartTime().getTimestamp()).build())
              .addAllExpectedEvaluatorIds(restart.getExpectedEvaluatorIds())
              .build());
        } else {
          stop(new DriverClientException("Failed to restart driver client"));
        }
      }
    } catch (InterruptedException | IOException e) {
      stop(e);
    }
  }

  @Override
  public void restartRunningTask(final RunningTask task) {
    synchronized (this) {
      final ActiveContext context = task.getActiveContext();
      if (!this.activeContextMap.containsKey(context.getId())) {
        this.activeContextMap.put(context.getId(), context);
      }
      this.runningTaskMap.put(task.getId(), task);
      this.clientStub.driverRestartRunningTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContext(ContextInfo.newBuilder()
                  .setContextId(context.getId())
                  .setEvaluatorId(context.getEvaluatorId())
                  .setParentId(context.getParentId().isPresent() ? context.getParentId().get() : "")
                  .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(context.getEvaluatorDescriptor()))
                  .build())
              .build());
    }
  }

  @Override
  public void restartActiveContext(final ActiveContext context) {
    synchronized (this) {
      this.activeContextMap.put(context.getId(), context);
      this.clientStub.driverRestartActiveContextHandler(
          ContextInfo.newBuilder()
              .setContextId(context.getId())
              .setEvaluatorId(context.getEvaluatorId())
              .setParentId(
                  context.getParentId().isPresent() ?
                      context.getParentId().get() : null)
              .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
                  context.getEvaluatorDescriptor()))
              .build());
    }
  }

  @Override
  public void driverRestartCompleted(final DriverRestartCompleted restartCompleted) {
    synchronized (this) {
      this.clientStub.driverRestartCompletedHandler(DriverRestartCompletedInfo.newBuilder()
          .setCompletionTime(StopTimeInfo.newBuilder()
              .setStopTime(restartCompleted.getCompletedTime().getTimestamp()).build())
          .setIsTimedOut(restartCompleted.isTimedOut())
          .build());
    }
  }

  @Override
  public void restartFailedEvalautor(final FailedEvaluator evaluator) {
    synchronized (this) {
      this.clientStub.driverRestartFailedEvaluatorHandler(EvaluatorInfo.newBuilder()
          .setEvaluatorId(evaluator.getId())
          .setFailure(EvaluatorInfo.FailureInfo.newBuilder()
              .setMessage(evaluator.getEvaluatorException() != null ?
                  evaluator.getEvaluatorException().getMessage() : "unknown failure during restart")
              .build())
          .build());
    }
  }

  private final class DriverBridgeServiceImpl
      extends DriverServiceGrpc.DriverServiceImplBase {

    @Override
    public void registerDriverClient(
        final DriverClientRegistration request,
        final StreamObserver<Void> responseObserver) {
      try {
        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress(request.getHost(), request.getPort())
            .usePlaintext(true)
            .build();
        synchronized (GRPCDriverService.this) {
          GRPCDriverService.this.clientStub = DriverClientGrpc.newFutureStub(channel);
          GRPCDriverService.this.notifyAll();
        }
      } finally {
        responseObserver.onNext(null);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void requestResources(
        final ResourceRequest request,
        final StreamObserver<Void> responseObserver) {
      try {
        synchronized (GRPCDriverService.this) {
          EvaluatorRequest.Builder requestBuilder = GRPCDriverService.this.evaluatorRequestor.newRequest();
          requestBuilder.setNumber(request.getResourceCount());
          requestBuilder.setNumberOfCores(request.getCores());
          requestBuilder.setMemory(request.getMemorySize());
          requestBuilder.setRelaxLocality(request.getRelaxLocality());
          requestBuilder.setRuntimeName(request.getRuntimeName());
          if (request.getNodeNameListCount() > 0) {
            requestBuilder.addNodeNames(request.getNodeNameListList());
          }
          if (request.getRackNameListCount() > 0) {
            for (final String rackName : request.getRackNameListList()) {
              requestBuilder.addRackName(rackName);
            }
          }
          GRPCDriverService.this.evaluatorRequestor.submit(requestBuilder.build());
        }
      } finally {
        responseObserver.onNext(null);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void shutdown(
        final ShutdownRequest request,
        final StreamObserver<Void> responseObserver) {
      try {
        synchronized (GRPCDriverService.this) {
          if (request.getException() != null) {
            GRPCDriverService.this.clock.stop(
                new DriverClientException(request.getException().getMessage()));
          } else {
            GRPCDriverService.this.clock.stop();
          }
        }
      } finally {
        responseObserver.onNext(null);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void setAlarm(
        final AlarmRequest request,
        final StreamObserver<Void> responseObserver) {
      try {
        synchronized (GRPCDriverService.this) {
          GRPCDriverService.this.clock.scheduleAlarm(request.getTimeoutMs(), new EventHandler<Alarm>() {
            @Override
            public void onNext(final Alarm value) {
              synchronized (GRPCDriverService.this) {
                GRPCDriverService.this.clientStub.alarmTrigger(
                    AlarmTriggerInfo.newBuilder().setAlarmId(request.getAlarmId()).build());
              }
            }
          });
        }
      } finally {
        responseObserver.onNext(null);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void allocatedEvaluatorOp(
        final AllocatedEvaluatorRequest request,
        final StreamObserver<Void> responseObserver) {
      try {
        if (request.getEvaluatorConfiguration() == null) {
          responseObserver.onError(Status.INTERNAL
              .withDescription("Evaluator configuration required")
              .asRuntimeException());
        } else if (request.getContextConfiguration() == null && request.getTaskConfiguration() == null) {
          responseObserver.onError(Status.INTERNAL
              .withDescription("Context and/or Task configuration required")
              .asRuntimeException());
        } else {
          synchronized (GRPCDriverService.this) {
            if (!GRPCDriverService.this.allocatedEvaluatorMap.containsKey(request.getEvaluatorId())) {
              responseObserver.onError(Status.INTERNAL
                  .withDescription("Unknown allocated evaluator " + request.getEvaluatorId())
                  .asRuntimeException());
            }
            final AllocatedEvaluator evaluator =
                GRPCDriverService.this.allocatedEvaluatorMap.get(request.getEvaluatorId());
            if (request.getCloseEvaluator()) {
              evaluator.close();
            } else {
              if (request.getAddFilesCount() > 0) {
                for (final String file : request.getAddFilesList()) {
                  evaluator.addFile(new File(file));
                }
              }
              if (request.getAddLibrariesCount() > 0) {
                for (final String library : request.getAddLibrariesList()) {
                  evaluator.addLibrary(new File(library));
                }
              }
              if (request.getSetProcess() != null) {
                final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest =
                    request.getSetProcess();
                switch (evaluator.getEvaluatorDescriptor().getProcess().getType()) {
                case JVM:
                  setJVMProcess(evaluator, processRequest);
                  break;
                case CLR:
                  setCLRProcess(evaluator, processRequest);
                  break;
                default:
                  throw new RuntimeException("Unknown evaluator process type");
                }
              }
              if (StringUtils.isEmpty(request.getEvaluatorConfiguration())) {
                // Assume that we are running Java driver client, but this assumption could be a bug so log a warning
                LOG.log(Level.WARNING, "No evaluator configuration detected. Assuming a Java driver client.");
                if (request.getContextConfiguration() != null && request.getTaskConfiguration() != null) {
                  // submit context and task
                  Validate.notEmpty(request.getContextConfiguration(), "Context configuration not set");
                  Validate.notEmpty(request.getTaskConfiguration(), "Task configuration not set");
                  try {
                    evaluator.submitContextAndTask(
                        configurationSerializer.fromString(request.getContextConfiguration()),
                        configurationSerializer.fromString(request.getTaskConfiguration()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                } else if (request.getContextConfiguration() != null) {
                  // submit context
                  Validate.notEmpty(request.getContextConfiguration(), "Context configuration not set");
                  try {
                    evaluator.submitContext(configurationSerializer.fromString(request.getContextConfiguration()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                } else if (request.getTaskConfiguration() != null) {
                  // submit task
                  Validate.notEmpty(request.getTaskConfiguration(), "Task configuration not set");
                  try {
                    evaluator.submitTask(configurationSerializer.fromString(request.getTaskConfiguration()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                } else {
                  throw new RuntimeException("Missing check for required evaluator configurations");
                }
              } else {
                if (request.getContextConfiguration() != null && request.getTaskConfiguration() != null) {
                  // submit context and task
                  Validate.notEmpty(request.getEvaluatorConfiguration(), "Evaluator configuration not set");
                  Validate.notEmpty(request.getContextConfiguration(), "Context configuration not set");
                  Validate.notEmpty(request.getTaskConfiguration(), "Task configuration not set");
                  ((AllocatedEvaluatorImpl) evaluator).submitContextAndTask(
                      request.getEvaluatorConfiguration(),
                      request.getContextConfiguration(),
                      request.getTaskConfiguration());
                } else if (request.getContextConfiguration() != null) {
                  // submit context
                  Validate.notEmpty(request.getEvaluatorConfiguration(), "Evaluator configuration not set");
                  Validate.notEmpty(request.getContextConfiguration(), "Context configuration not set");
                  ((AllocatedEvaluatorImpl) evaluator).submitContext(
                      request.getEvaluatorConfiguration(),
                      request.getContextConfiguration());
                } else if (request.getTaskConfiguration() != null) {
                  // submit task
                  Validate.notEmpty(request.getEvaluatorConfiguration(), "Evaluator configuration not set");
                  Validate.notEmpty(request.getTaskConfiguration(), "Task configuration not set");
                  ((AllocatedEvaluatorImpl) evaluator).submitTask(
                      request.getEvaluatorConfiguration(),
                      request.getTaskConfiguration());
                } else {
                  throw new RuntimeException("Missing check for required evaluator configurations");
                }
              }
            }
          }
        }
      } finally {
        responseObserver.onNext(null);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void activeContextOp(
        final ActiveContextRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        if (!GRPCDriverService.this.activeContextMap.containsKey(request.getContextId())) {
          LOG.log(Level.SEVERE, "Context does not exist with id " + request.getContextId());
          responseObserver.onError(Status.INTERNAL
              .withDescription("Context does not exist with id " + request.getContextId())
              .asRuntimeException());
        } else {
          final ActiveContext context = GRPCDriverService.this.activeContextMap.get(request.getContextId());
          if (request.getOperationCase() == ActiveContextRequest.OperationCase.CLOSE_CONTEXT) {
            if (request.getCloseContext()) {
              try {
                LOG.log(Level.INFO, "closing context " + context.getId());
                context.close();
              } finally {
                responseObserver.onNext(null);
                responseObserver.onCompleted();
              }
            } else {
              LOG.log(Level.SEVERE, "Close context operation not set to true");
              responseObserver.onError(Status.INTERNAL
                  .withDescription("Close context operation not set to true")
                  .asRuntimeException());
            }
          } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.MESSAGE) {
            if (request.getMessage() != null) {
              try {
                LOG.log(Level.INFO, "send message to context " + context.getId());
                context.sendMessage(request.getMessage().toByteArray());
              } finally {
                responseObserver.onNext(null);
                responseObserver.onCompleted();
              }
            } else {
              responseObserver.onError(Status.INTERNAL
                  .withDescription("Empty message on operation send message").asRuntimeException());
            }
          } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.NEW_CONTEXT_REQUEST) {
            try {
              LOG.log(Level.INFO, "submitting child context to context " + context.getId());
              ((EvaluatorContext) context).submitContext(request.getNewContextRequest());
            } finally {
              responseObserver.onNext(null);
              responseObserver.onCompleted();
            }
          } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.NEW_TASK_REQUEST) {
            try {
              LOG.log(Level.INFO, "submitting task to context " + context.getId());
              ((EvaluatorContext) context).submitTask(request.getNewTaskRequest());
            } finally {
              responseObserver.onNext(null);
              responseObserver.onCompleted();
            }
          }
        }
      }
    }

    @Override
    public void runningTaskOp(
        final RunningTaskRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        if (!GRPCDriverService.this.runningTaskMap.containsKey(request.getTaskId())) {
          responseObserver.onError(Status.INTERNAL
              .withDescription("Task does not exist with id " + request.getTaskId()).asRuntimeException());
        }
        try {
          final RunningTask task = GRPCDriverService.this.runningTaskMap.get(request.getTaskId());
          if (request.getCloseTask()) {
            if (request.getMessage() != null) {
              task.close(request.getMessage().toByteArray());
            } else {
              task.close();
            }
          } else if (request.getMessage() != null) {
            task.send(request.getMessage().toByteArray());
          }
        } finally {
          responseObserver.onNext(null);
          responseObserver.onCompleted();
        }
      }
    }

    private void setCLRProcess(
        final AllocatedEvaluator evaluator,
        final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest) {
      final CLRProcess process = GRPCDriverService.this.clrProcessFactory.newEvaluatorProcess();
      if (processRequest.getMemoryMb() > 0) {
        process.setMemory(processRequest.getMemoryMb());
      }
      if (processRequest.getConfigurationFileName() != null) {
        process.setConfigurationFileName(processRequest.getConfigurationFileName());
      }
      if (processRequest.getStandardOut() != null) {
        process.setStandardOut(processRequest.getStandardOut());
      }
      if (processRequest.getStandardErr() != null) {
        process.setStandardErr(processRequest.getStandardErr());
      }
      evaluator.setProcess(process);
    }

    private void setJVMProcess(
        final AllocatedEvaluator evaluator,
        final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest) {
      final JVMProcess process = GRPCDriverService.this.jvmProcessFactory.newEvaluatorProcess();
      if (processRequest.getMemoryMb() > 0) {
        process.setMemory(processRequest.getMemoryMb());
      }
      if (processRequest.getConfigurationFileName() != null) {
        process.setConfigurationFileName(processRequest.getConfigurationFileName());
      }
      if (processRequest.getStandardOut() != null) {
        process.setStandardOut(processRequest.getStandardOut());
      }
      if (processRequest.getStandardErr() != null) {
        process.setStandardErr(processRequest.getStandardErr());
      }
      if (processRequest.getOptionsCount() > 0) {
        for (final String option : processRequest.getOptionsList()) {
          process.addOption(option);
        }
      }
      evaluator.setProcess(process);
    }
  }
}
