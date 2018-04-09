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
package org.apache.reef.bridge.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.reef.bridge.IDriverService;
import org.apache.reef.bridge.parameters.DriverClientProcessCommand;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.AllocatedEvaluatorImpl;
import org.apache.reef.tang.annotations.Parameter;
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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GRPC DriverBridgeService that interacts with higher-level languages.
 */
public final class GRPCDriverService implements IDriverService {
  private static final Logger LOG = Logger.getLogger(GRPCDriverService.class.getName());

  private Server server;

  private Process driverProcess;

  private DriverClientGrpc.DriverClientBlockingStub clientStub;

  private final Clock clock;

  private final EvaluatorRequestor evaluatorRequestor;

  private final JVMProcessFactory jvmProcessFactory;

  private final CLRProcessFactory clrProcessFactory;

  private final TcpPortProvider tcpPortProvider;

  private final String driverProcessCommand;

  private final Map<String, AllocatedEvaluator> allocatedEvaluatorMap = new HashMap<>();

  private final Map<String, ActiveContext> activeContextMap = new HashMap<>();

  private final Map<String, RunningTask> runningTaskMap = new HashMap<>();

  @Inject
  private GRPCDriverService(
      final Clock clock,
      final EvaluatorRequestor evaluatorRequestor,
      final JVMProcessFactory jvmProcessFactory,
      final CLRProcessFactory clrProcessFactory,
      final TcpPortProvider tcpPortProvider,
      @Parameter(DriverClientProcessCommand.class) final String driverProcessCommand) {
    this.clock = clock;
    this.jvmProcessFactory = jvmProcessFactory;
    this.clrProcessFactory = clrProcessFactory;
    this.evaluatorRequestor = evaluatorRequestor;
    this.driverProcessCommand = driverProcessCommand;
    this.tcpPortProvider = tcpPortProvider;
  }

  private void start() throws IOException {
    for (final Integer port : this.tcpPortProvider) {
      try {
        this.server = ServerBuilder.forPort(port)
            .addService(new DriverBridgeServiceImpl())
            .build()
            .start();
        LOG.info("Server started, listening on " + port);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Unable to bind to port [{0}]", port);
      }
    }
    if (this.server == null || this.server.isTerminated()) {
      throw new IOException("Unable to start gRPC server");
    } else {
      final String cmd = this.driverProcessCommand + " -server-port=" + this.server.getPort();
      final String cmdOs = OSUtils.isWindows() ? "cmd.exe /c " + cmd : cmd;
      this.driverProcess = Runtime.getRuntime().exec(cmdOs);
    }
  }

  private void stop() {
    if (server != null) {
      this.server.shutdown();
      this.server = null;
    }
    if (this.driverProcess != null) {
      this.driverProcess.destroy();
      this.driverProcess = null;
    }
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
  public void startHandler(final StartTime startTime) {
    try {
      start();
      synchronized (this) {
        // wait for driver client process to register
        while (this.clientStub == null && driverProcessIsAlive()) {
          this.wait();
        }
        if (this.clientStub != null) {
          this.clientStub.startHandler(
              StartTimeInfo.newBuilder().setStartTime(startTime.getTimestamp()).build());
        }
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      stop();
    }
  }

  @Override
  public void stopHandler(final StopTime stopTime) {
    synchronized (this) {
      try {
        this.clientStub.stopHandler(
            StopTimeInfo.newBuilder().setStopTime(stopTime.getTimestamp()).build());
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
      this.runningTaskMap.put(task.getId(), task);
      this.clientStub.runningTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContextId(task.getActiveContext().getId())
              .build());
    }
  }

  @Override
  public void failedTaskHandler(final FailedTask task) {
    synchronized (this) {
      this.runningTaskMap.remove(task.getId());
      this.clientStub.failedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContextId(
                  task.getActiveContext().isPresent() ?
                      task.getActiveContext().get().getId() : null)
              .build());
    }
  }

  @Override
  public void completedTaskHandler(final CompletedTask task) {
    synchronized (this) {
      this.runningTaskMap.remove(task.getId());
      this.clientStub.completedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContextId(task.getActiveContext().getId())
              .build());
    }
  }

  @Override
  public void suspendedTaskHandler(final SuspendedTask task) {
    synchronized (this) {
      this.runningTaskMap.remove(task.getId());
      this.clientStub.suspendedTaskHandler(
          TaskInfo.newBuilder()
              .setTaskId(task.getId())
              .setContextId(task.getActiveContext().getId())
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
      this.clientStub.clientCloseHandler(
          Void.newBuilder().build());
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

  private final class DriverBridgeServiceImpl
      extends DriverServiceGrpc.DriverServiceImplBase {

    @Override
    public void registerDriverClient(
        final DriverClientRegistration request,
        final StreamObserver<Void> responseObserver) {
      final ManagedChannel channel = ManagedChannelBuilder
          .forAddress(request.getHost(), request.getPort())
          .usePlaintext(true)
          .build();
      synchronized (GRPCDriverService.this) {
        GRPCDriverService.this.clientStub = DriverClientGrpc.newBlockingStub(channel);
        GRPCDriverService.this.notifyAll();
      }
      responseObserver.onNext(Void.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void requestResources(
        final ResourceRequest request,
        final StreamObserver<Void> responseObserver) {
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
      responseObserver.onNext(Void.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void shutdown(
        final ShutdownRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        GRPCDriverService.this.clock.stop();
      }
      responseObserver.onNext(Void.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void setAlarm(
        final AlarmRequest request,
        final StreamObserver<Void> responseObserver) {
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
    }

    @Override
    public void allocatedEvaluatorOp(
        final AllocatedEvaluatorRequest request,
        final StreamObserver<Void> responseObserver) {
      try {
        if (request.getEvaluatorConfiguration() == null) {
          responseObserver.onError(
              new IllegalArgumentException("Evaluator configuration required"));
        } else if (request.getContextConfiguration() == null && request.getTaskConfiguration() == null) {
          responseObserver.onError(
              new IllegalArgumentException("Context and/or Task configuration required"));
        } else {
          synchronized (GRPCDriverService.this) {
            if (!GRPCDriverService.this.allocatedEvaluatorMap.containsKey(request.getEvaluatorId())) {
              responseObserver.onError(
                  new IllegalArgumentException("Unknown allocated evaluator " + request.getEvaluatorId()));
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
              if (request.getContextConfiguration() != null && request.getTaskConfiguration() != null) {
                // submit context and task
                ((AllocatedEvaluatorImpl) evaluator).submitContextAndTask(
                    request.getEvaluatorConfiguration(),
                    request.getContextConfiguration(),
                    request.getTaskConfiguration());
              } else if (request.getContextConfiguration() != null) {
                // submit context
                ((AllocatedEvaluatorImpl) evaluator).submitContext(
                    request.getEvaluatorConfiguration(),
                    request.getContextConfiguration());
              } else if (request.getTaskConfiguration() != null) {
                // submit task
                ((AllocatedEvaluatorImpl) evaluator).submitTask(
                    request.getEvaluatorConfiguration(),
                    request.getTaskConfiguration());
              } else {
                throw new RuntimeException("Missing check for required evaluator configurations");
              }
              responseObserver.onNext(Void.newBuilder().build());
            }
          }
        }
      } finally {
        responseObserver.onCompleted();
      }
    }

    @Override
    public void activeContextOp(
        final ActiveContextRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        if (!GRPCDriverService.this.activeContextMap.containsKey(request.getContextId())) {
          responseObserver.onError(
              new IllegalArgumentException("Context does not exist with id " + request.getContextId()));
        } else if (request.getNewContextRequest() != null && request.getNewTaskRequest() != null) {
          responseObserver.onError(
              new IllegalArgumentException("Context request can only contain one of a context or task configuration"));

        }
        final ActiveContext context = GRPCDriverService.this.activeContextMap.get(request.getContextId());
        if (request.getOperationCase() == ActiveContextRequest.OperationCase.CLOSE_CONTEXT) {
          if (request.getCloseContext()) {
            context.close();
          } else {
            responseObserver.onError(new IllegalArgumentException("Close context operation not set to true"));
          }
        } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.MESSAGE) {
          if (request.getMessage() != null) {
            context.sendMessage(request.getMessage().toByteArray());
          } else {
            responseObserver.onError(new IllegalArgumentException("Empty message on operation send message"));
          }
        } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.NEW_CONTEXT_REQUEST) {
          ((EvaluatorContext) context).submitContext(request.getNewContextRequest());
        } else if (request.getOperationCase() == ActiveContextRequest.OperationCase.NEW_TASK_REQUEST) {
          ((EvaluatorContext) context).submitTask(request.getNewTaskRequest());
        }
      }
    }

    @Override
    public void runningTaskOp(
        final RunningTaskRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        if (!GRPCDriverService.this.runningTaskMap.containsKey(request.getTaskId())) {
          responseObserver.onError(
              new IllegalArgumentException("Task does not exist with id " + request.getTaskId()));
        }
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
