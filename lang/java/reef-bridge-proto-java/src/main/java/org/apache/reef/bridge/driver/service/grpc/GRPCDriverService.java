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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.common.grpc.GRPCUtils;
import org.apache.reef.bridge.driver.common.grpc.ObserverCleanup;
import org.apache.reef.bridge.driver.service.DriverService;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.bridge.service.parameters.DriverClientCommand;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.exception.NonSerializableException;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.AllocatedEvaluatorImpl;
import org.apache.reef.runtime.common.driver.idle.DriverIdlenessSource;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.OSUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.*;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GRPC DriverBridgeService that interacts with higher-level languages.
 */
@Private
public final class GRPCDriverService implements DriverService, DriverIdlenessSource {
  private static final Logger LOG = Logger.getLogger(GRPCDriverService.class.getName());

  private static final Void VOID = Void.newBuilder().build();

  private Process driverProcess;

  private enum StreamType { STDOUT, STDERR }

  private Server server;

  private DriverClientGrpc.DriverClientFutureStub clientStub;

  private final Clock clock;

  private final REEFFileNames reefFileNames;

  private final ExceptionCodec exceptionCodec;

  private final EvaluatorRequestor evaluatorRequestor;

  private final JVMProcessFactory jvmProcessFactory;

  private final CLRProcessFactory clrProcessFactory;

  private final DotNetProcessFactory dotNetProcessFactory;

  private final TcpPortProvider tcpPortProvider;

  private final String driverClientCommand;

  private final Map<String, AllocatedEvaluator> allocatedEvaluatorMap = new HashMap<>();

  private final Map<String, ActiveContext> activeContextMap = new HashMap<>();

  private final Map<String, RunningTask> runningTaskMap = new HashMap<>();

  private boolean stopped = false;

  @Inject
  private GRPCDriverService(
      final Clock clock,
      final REEFFileNames reefFileNames,
      final EvaluatorRequestor evaluatorRequestor,
      final JVMProcessFactory jvmProcessFactory,
      final CLRProcessFactory clrProcessFactory,
      final DotNetProcessFactory dotNetProcessFactory,
      final TcpPortProvider tcpPortProvider,
      final ExceptionCodec exceptionCodec,
      @Parameter(DriverClientCommand.class) final String driverClientCommand) {
    this.clock = clock;
    this.reefFileNames = reefFileNames;
    this.exceptionCodec = exceptionCodec;
    this.jvmProcessFactory = jvmProcessFactory;
    this.clrProcessFactory = clrProcessFactory;
    this.dotNetProcessFactory = dotNetProcessFactory;
    this.evaluatorRequestor = evaluatorRequestor;
    this.driverClientCommand = driverClientCommand;
    this.tcpPortProvider = tcpPortProvider;
  }

  private void start() throws IOException, InterruptedException {
    for (final int port : this.tcpPortProvider) {
      try {
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", port))
            .addService(new DriverBridgeServiceImpl())
            .build()
            .start();
        LOG.log(Level.INFO, "Server started, listening on port [{0}]", port);
        break;
      } catch (final IOException e) {
        LOG.log(Level.WARNING, "Unable to bind to port [{0}]", port);
      }
    }
    if (this.server == null || this.server.isTerminated()) {
      throw new IOException("Unable to start gRPC server");
    }
    final String cmd = this.driverClientCommand + " " + this.server.getPort();
    final List<String> cmdOs = OSUtils.isWindows() ?
        Arrays.asList("cmd.exe", "/c", cmd) : Arrays.asList("/bin/sh", "-c", cmd);
    LOG.log(Level.INFO, "CMD: {0}", cmdOs);
    this.driverProcess = new ProcessBuilder()
        .command(cmdOs)
        .redirectError(new File(this.reefFileNames.getDriverClientStderrFileName()))
        .redirectOutput(new File(this.reefFileNames.getDriverClientStdoutFileName()))
        .start();
    synchronized (this) {
      int attempts = 60; // give it a minute
      /* wait for driver client process to register
       * Note: attempts and wait time have been given reasonable hardcoded values for a driver
       * client to register with the driver service (us). Making these values configurable would
       * require additions to the ClientProtocol buffer such that they can be passed to the
       * GRPCDriverServiceConfigurationProvider and bound to the appropriate NamedParameters. It
       * is the opinion at the time of this writing that a driver client should be able to register
       * within a minute.
       */
      while (!stopped && attempts-- > 0 && this.clientStub == null && driverProcessIsAlive()) {
        LOG.log(Level.INFO, "waiting for driver process to register");
        this.wait(1000); // a second
      }
    }
    if (!stopped && driverProcessIsAlive()) {
      final Thread closeChildThread = new Thread() {
        public void run() {
          synchronized (GRPCDriverService.this) {
            if (GRPCDriverService.this.driverProcess != null) {
              GRPCDriverService.this.driverProcess.destroy();
              GRPCDriverService.this.driverProcess = null;
            }
          }
        }
      };
      // This is probably overkill since shutdown should be called in the stop handler.
      Runtime.getRuntime().addShutdownHook(closeChildThread);
    }
  }

  private void stop() {
    stop(null);
  }

  private void stop(final Throwable t) {
    LOG.log(Level.INFO, "STOP: gRPC Driver Service", t);
    if (!stopped) {
      try {
        if (!clock.isClosed()) {
          if (t != null) {
            clock.stop(t);
          } else {
            clock.stop();
          }
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
        clientStub = null;
        stopped = true;
      }
    }
  }

  private void dump() {
    if (!driverProcessIsAlive()) {
      LOG.log(Level.INFO, "Exit code: {0}", this.driverProcess.exitValue());
    }
    dumpStream(StreamType.STDOUT);
    dumpStream(StreamType.STDERR);
  }

  private void dumpStream(final StreamType type) {
    final StringBuilder stringBuilder = new StringBuilder();

    final String name;
    final InputStream stream;
    switch(type) {
    case STDOUT:
      name = "stdout";
      stream = this.driverProcess.getInputStream();
      break;
    case STDERR:
      name = "stderr";
      stream = this.driverProcess.getErrorStream();
      break;
    default:
      throw new RuntimeException("Invalid stream type value");
    }

    LOG.log(Level.INFO, "capturing driver process {0}", name);
    try {
      stringBuilder.append("\n==============================================\n");
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
        while (reader.ready()) {
          stringBuilder.append(reader.readLine()).append('\n');
        }
      }
      stringBuilder.append("\n==============================================\n");
    } catch (final IOException e) {
      LOG.log(Level.WARNING, "Error while capturing output stream", e);
    }
    LOG.log(Level.INFO, "{0}", stringBuilder);
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
      } catch (final IllegalThreadStateException e) {
        return true;
      }
    }
    return false;
  }


  @Override
  public IdleMessage getIdleStatus() {
    final String componentName = "Java Bridge DriverService";
    synchronized (this) {
      if (this.clientStub != null) {
        try {
          LOG.log(Level.INFO, "{0} getting idle status", componentName);
          final IdleStatus idleStatus = this.clientStub.idlenessCheckHandler(VOID).get();
          LOG.log(Level.INFO, "is idle: {0}", idleStatus.getIsIdle());
          return new IdleMessage(
              componentName,
              idleStatus.getReason(),
              idleStatus.getIsIdle());
        } catch (final ExecutionException | InterruptedException e) {
          stop(e);
        }
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
      return new IdleMessage(
          componentName,
          "stub not initialized",
          true);
    }
  }

  @Override
  public void startHandler(final StartTime startTime) {
    try {
      start();
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException("unable to start driver client", e);
    }
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.startHandler(
            StartTimeInfo.newBuilder().setStartTime(startTime.getTimestamp()).build());
      } else if (!stopped) {
        stop(new RuntimeException("Unable to start driver client"));
      }
    }
  }

  @Override
  public void stopHandler(final StopTime stopTime) {
    synchronized (this) {
      if (clientStub != null) {
        LOG.log(Level.INFO, "Stop handler called at {0}", stopTime);
        final Future<ExceptionInfo> callCompletion = this.clientStub.stopHandler(
            StopTimeInfo.newBuilder().setStopTime(stopTime.getTimestamp()).build());
        try {
          final ExceptionInfo error = callCompletion.get(5L, TimeUnit.MINUTES);
          if (!error.getNoError()) {
            final Optional<Throwable> t = parseException(error);
            if (t.isPresent()) {
              throw new RuntimeException("driver stop exception",
                  t.get().getCause() != null ? t.get().getCause() : t.get());
            } else {
              throw new RuntimeException(error.getName(),
                  new NonSerializableException(error.getMessage(), error.getData().toByteArray()));
            }
          }
        } catch (final TimeoutException e) {
          throw new RuntimeException("stop handler timed out", e);
        } catch (final InterruptedException | ExecutionException e) {
          throw new RuntimeException("error in stop handler", e);
        } finally {
          stop();
        }
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void allocatedEvaluatorHandler(final AllocatedEvaluator eval) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.allocatedEvaluatorMap.put(eval.getId(), eval);
        this.clientStub.allocatedEvaluatorHandler(
            EvaluatorInfo.newBuilder()
                .setEvaluatorId(eval.getId())
                .setDescriptorInfo(
                    GRPCUtils.toEvaluatorDescriptorInfo(eval.getEvaluatorDescriptor()))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void completedEvaluatorHandler(final CompletedEvaluator eval) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.allocatedEvaluatorMap.remove(eval.getId());
        this.clientStub.completedEvaluatorHandler(
            EvaluatorInfo.newBuilder().setEvaluatorId(eval.getId()).build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void failedEvaluatorHandler(final FailedEvaluator eval) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.allocatedEvaluatorMap.remove(eval.getId());
        this.clientStub.failedEvaluatorHandler(
            EvaluatorInfo.newBuilder().setEvaluatorId(eval.getId()).build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void activeContextHandler(final ActiveContext context) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.activeContextMap.put(context.getId(), context);
        this.clientStub.activeContextHandler(GRPCUtils.toContextInfo(context));
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void closedContextHandler(final ClosedContext context) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.activeContextMap.remove(context.getId());
        this.clientStub.closedContextHandler(GRPCUtils.toContextInfo(context));
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void failedContextHandler(final FailedContext context) {
    synchronized (this) {
      if (this.clientStub != null) {
        final ExceptionInfo error;
        if (context.getReason().isPresent()) {
          final Throwable reason = context.getReason().get();
          error = GRPCUtils.createExceptionInfo(this.exceptionCodec, reason);
        } else if (context.getData().isPresent()) {
          error = ExceptionInfo.newBuilder()
              .setName(context.toString())
              .setMessage(context.getDescription().orElse(
                  context.getMessage() != null ? context.getMessage() : ""))
              .setData(ByteString.copyFrom(context.getData().get()))
              .build();
        } else {
          error = GRPCUtils.createExceptionInfo(this.exceptionCodec, context.asError());
        }
        this.activeContextMap.remove(context.getId());
        this.clientStub.failedContextHandler(GRPCUtils.toContextInfo(context, error));
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void contextMessageHandler(final ContextMessage message) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.contextMessageHandler(
            ContextMessageInfo.newBuilder()
                .setContextId(message.getId())
                .setMessageSourceId(message.getMessageSourceID())
                .setSequenceNumber(message.getSequenceNumber())
                .setPayload(ByteString.copyFrom(message.get()))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void runningTaskHandler(final RunningTask task) {
    synchronized (this) {
      if (this.clientStub != null) {
        final ActiveContext context = task.getActiveContext();
        if (!this.activeContextMap.containsKey(context.getId())) {
          this.activeContextMap.put(context.getId(), context);
        }
        this.runningTaskMap.put(task.getId(), task);
        this.clientStub.runningTaskHandler(
            TaskInfo.newBuilder()
                .setTaskId(task.getId())
                .setContext(GRPCUtils.toContextInfo(context))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void failedTaskHandler(final FailedTask task) {
    synchronized (this) {
      if (this.clientStub != null) {
        if (task.getActiveContext().isPresent() &&
            !this.activeContextMap.containsKey(task.getActiveContext().get().getId())) {
          this.activeContextMap.put(task.getActiveContext().get().getId(), task.getActiveContext().get());
        }
        final TaskInfo.Builder taskInfoBuilder = TaskInfo.newBuilder()
            .setTaskId(task.getId());
        if (task.getActiveContext().isPresent()) {
          taskInfoBuilder.setContext(GRPCUtils.toContextInfo(task.getActiveContext().get()));
        }
        if (task.getReason().isPresent()) {
          LOG.log(Level.WARNING, "Task exception present", task.getReason().get());
          taskInfoBuilder.setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, task.getReason().get()));
        } else if (task.getData().isPresent()) {
          LOG.log(Level.WARNING, "Not able to deserialize task exception {0}", task.getMessage());
          final Throwable reason = task.asError();
          taskInfoBuilder.setException(ExceptionInfo.newBuilder()
              .setName(reason.toString())
              .setMessage(StringUtils.isNotEmpty(task.getMessage()) ? task.getMessage() : reason.toString())
              .setData(ByteString.copyFrom(task.getData().get()))
              .build());
        } else {
          LOG.log(Level.WARNING, "Serialize generic error");
          taskInfoBuilder.setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, task.asError()));
        }
        this.runningTaskMap.remove(task.getId());
        this.clientStub.failedTaskHandler(taskInfoBuilder.build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void completedTaskHandler(final CompletedTask task) {
    synchronized (this) {
      if (this.clientStub != null) {
        if (!this.activeContextMap.containsKey(task.getActiveContext().getId())) {
          this.activeContextMap.put(task.getActiveContext().getId(), task.getActiveContext());
        }
        this.runningTaskMap.remove(task.getId());
        this.clientStub.completedTaskHandler(
            TaskInfo.newBuilder()
                .setTaskId(task.getId())
                .setContext(GRPCUtils.toContextInfo(task.getActiveContext()))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void suspendedTaskHandler(final SuspendedTask task) {
    synchronized (this) {
      if (this.clientStub != null) {
        if (!this.activeContextMap.containsKey(task.getActiveContext().getId())) {
          this.activeContextMap.put(task.getActiveContext().getId(), task.getActiveContext());
        }
        this.runningTaskMap.remove(task.getId());
        this.clientStub.suspendedTaskHandler(
            TaskInfo.newBuilder()
                .setTaskId(task.getId())
                .setContext(GRPCUtils.toContextInfo(task.getActiveContext()))
                .setResult(task.get() == null || task.get().length == 0 ?
                    null : ByteString.copyFrom(task.get()))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void taskMessageHandler(final TaskMessage message) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.taskMessageHandler(
            TaskMessageInfo.newBuilder()
                .setTaskId(message.getId())
                .setContextId(message.getContextId())
                .setMessageSourceId(message.getMessageSourceID())
                .setSequenceNumber(message.getSequenceNumber())
                .setPayload(ByteString.copyFrom(message.get()))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void clientMessageHandler(final byte[] message) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.clientMessageHandler(
            ClientMessageInfo.newBuilder()
                .setPayload(ByteString.copyFrom(message))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void clientCloseHandler() {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.clientCloseHandler(VOID);
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void clientCloseWithMessageHandler(final byte[] message) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.clientCloseWithMessageHandler(
            ClientMessageInfo.newBuilder()
                .setPayload(ByteString.copyFrom(message))
                .build());
      } else {
        LOG.log(Level.WARNING, "client shutdown has already completed");
      }
    }
  }

  @Override
  public void driverRestarted(final DriverRestarted restart) {
    try {
      start();
    } catch (final InterruptedException | IOException e) {
      throw new RuntimeException("unable to start driver client", e);
    }
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.driverRestartHandler(DriverRestartInfo.newBuilder()
            .setResubmissionAttempts(restart.getResubmissionAttempts())
            .setStartTime(StartTimeInfo.newBuilder()
                .setStartTime(restart.getStartTime().getTimestamp()).build())
            .addAllExpectedEvaluatorIds(restart.getExpectedEvaluatorIds())
            .build());
      } else {
        throw new RuntimeException("client stub not running");
      }
    }
  }

  @Override
  public void restartRunningTask(final RunningTask task) {
    synchronized (this) {
      if (this.clientStub != null) {
        final ActiveContext context = task.getActiveContext();
        if (!this.activeContextMap.containsKey(context.getId())) {
          this.activeContextMap.put(context.getId(), context);
        }
        this.runningTaskMap.put(task.getId(), task);
        this.clientStub.driverRestartRunningTaskHandler(
            TaskInfo.newBuilder()
                .setTaskId(task.getId())
                .setContext(GRPCUtils.toContextInfo(context))
                .build());
      } else {
        throw new RuntimeException("client stub not running");
      }
    }
  }

  @Override
  public void restartActiveContext(final ActiveContext context) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.activeContextMap.put(context.getId(), context);
        this.clientStub.driverRestartActiveContextHandler(
            GRPCUtils.toContextInfo(context));
      } else {
        throw new RuntimeException("client stub not running");
      }
    }
  }

  @Override
  public void driverRestartCompleted(final DriverRestartCompleted restartCompleted) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.driverRestartCompletedHandler(DriverRestartCompletedInfo.newBuilder()
            .setCompletionTime(StopTimeInfo.newBuilder()
                .setStopTime(restartCompleted.getCompletedTime().getTimestamp()).build())
            .setIsTimedOut(restartCompleted.isTimedOut())
            .build());
      } else {
        throw new RuntimeException("client stub not running");
      }
    }
  }

  @Override
  public void restartFailedEvalautor(final FailedEvaluator evaluator) {
    synchronized (this) {
      if (this.clientStub != null) {
        this.clientStub.driverRestartFailedEvaluatorHandler(EvaluatorInfo.newBuilder()
            .setEvaluatorId(evaluator.getId())
            .setFailure(EvaluatorInfo.FailureInfo.newBuilder()
                .setMessage(evaluator.getEvaluatorException() != null ?
                    evaluator.getEvaluatorException().getMessage() : "unknown failure during restart")
                .build())
            .build());
      } else {
        throw new RuntimeException("client stub not running");
      }
    }
  }

  private Optional<Throwable> parseException(final ExceptionInfo info) {
    if (info.getData().isEmpty()) {
      return Optional.empty();
    } else {
      return exceptionCodec.fromBytes(info.getData().toByteArray());
    }
  }

  private final class DriverBridgeServiceImpl
      extends DriverServiceGrpc.DriverServiceImplBase {

    @Override
    public void registerDriverClient(
        final DriverClientRegistration request,
        final StreamObserver<Void> responseObserver) {
      LOG.log(Level.INFO, "driver client register");
      synchronized (GRPCDriverService.this) {
        try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
          if (request.hasException()) {
            LOG.log(Level.SEVERE, "Driver client initialization exception");
            final Optional<Throwable> optionalEx = parseException(request.getException());
            final Throwable ex;
            if (optionalEx.isPresent()) {
              ex = optionalEx.get();
            } else if (!request.getException().getData().isEmpty()) {
              ex = new NonSerializableException(request.getException().getMessage(),
                  request.getException().getData().toByteArray());
            } else {
              ex = new RuntimeException(request.getException().getMessage());
            }
            stop(ex);
          } else {
            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(request.getHost(), request.getPort())
                .usePlaintext()
                .build();
            GRPCDriverService.this.clientStub = DriverClientGrpc.newFutureStub(channel);
            LOG.log(Level.INFO, "Driver has registered on port {0}", request.getPort());
          }
        } finally {
          GRPCDriverService.this.notifyAll();
        }
      }
    }

    @Override
    public void requestResources(
        final ResourceRequest request,
        final StreamObserver<Void> responseObserver) {
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
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
      }
    }

    @Override
    public void shutdown(
        final ShutdownRequest request,
        final StreamObserver<Void> responseObserver) {
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        LOG.log(Level.INFO, "driver shutdown");
        if (request.hasException()) {
          final Optional<Throwable> exception = parseException(request.getException());
          if (exception.isPresent()) {
            LOG.log(Level.INFO, "driver exception", exception.get());
            GRPCDriverService.this.clock.stop(exception.get());
          } else {
            // exception that cannot be parsed in java
            GRPCDriverService.this.clock.stop(
                new NonSerializableException(
                    request.getException().getMessage(),
                    request.getException().getData().toByteArray()));
          }
        } else {
          LOG.log(Level.INFO, "clean shutdown");
          GRPCDriverService.this.clock.stop();
        }
      }
    }

    @Override
    public void setAlarm(
        final AlarmRequest request,
        final StreamObserver<Void> responseObserver) {
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        // do not synchronize when scheduling an alarm (or deadlock)
        LOG.log(Level.INFO, "Set alarm {0} offset {1}",
            new Object[] {request.getAlarmId(), request.getTimeoutMs()});
        LOG.log(Level.INFO, "Alarm class {0}", GRPCDriverService.this.clock.getClass());
        GRPCDriverService.this.clock.scheduleAlarm(request.getTimeoutMs(), new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm value) {
            LOG.log(Level.INFO, "Trigger alarm {0}", request.getAlarmId());
            synchronized (GRPCDriverService.this) {
              GRPCDriverService.this.clientStub.alarmTrigger(
                  AlarmTriggerInfo.newBuilder().setAlarmId(request.getAlarmId()).build());
              LOG.log(Level.INFO, "DONE: trigger alarm {0}", request.getAlarmId());
            }
          }
        });
        LOG.log(Level.INFO, "Alarm {0} scheduled is idle? {1}",
            new Object[] {request.getAlarmId(), clock.isIdle()});
      }
    }

    @Override
    public void allocatedEvaluatorOp(
        final AllocatedEvaluatorRequest request,
        final StreamObserver<Void> responseObserver) {
      try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
        synchronized (GRPCDriverService.this) {
          final AllocatedEvaluator evaluator =
              GRPCDriverService.this.allocatedEvaluatorMap.get(request.getEvaluatorId());
          if (evaluator == null) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Unknown allocated evaluator " + request.getEvaluatorId())
                .asRuntimeException());
            return;
          }
          // Close evaluator?
          if (request.getCloseEvaluator()) {
            evaluator.close();
            return;
          }

          // Ensure context and/or task
          if (StringUtils.isEmpty(request.getContextConfiguration()) &&
              StringUtils.isEmpty(request.getTaskConfiguration())) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Context and/or Task configuration required")
                .asRuntimeException());
            return;
          }
          for (final String file : request.getAddFilesList()) {
            evaluator.addFile(new File(file));
          }
          for (final String library : request.getAddLibrariesList()) {
            evaluator.addLibrary(new File(library));
          }
          if (request.hasSetProcess()) {
            final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest =
                request.getSetProcess();
            switch (processRequest.getProcessType()) {
            case JVM:
              LOG.log(Level.INFO, "Setting JVM Process");
              setJVMProcess(evaluator, processRequest);
              break;
            case CLR:
              LOG.log(Level.INFO, "Setting CLR Process");
              setEvaluatorProcess(evaluator, clrProcessFactory.newEvaluatorProcess(), processRequest);
              break;
            case DOTNET:
              LOG.log(Level.INFO, "Setting DOTNET Process");
              setEvaluatorProcess(evaluator, dotNetProcessFactory.newEvaluatorProcess(), processRequest);
              break;
            default:
              throw new RuntimeException("Unknown evaluator process type");
            }
          }
          final String evaluatorConfiguration = StringUtils.isNotEmpty(request.getEvaluatorConfiguration()) ?
              request.getEvaluatorConfiguration() : null;
          final String contextConfiguration = StringUtils.isNotEmpty(request.getContextConfiguration()) ?
              request.getContextConfiguration() : null;
          final String serviceConfiguration = StringUtils.isNotEmpty(request.getServiceConfiguration()) ?
              request.getServiceConfiguration() : null;
          final String taskConfiguration = StringUtils.isNotEmpty(request.getTaskConfiguration()) ?
              request.getTaskConfiguration() : null;
          if (contextConfiguration != null && taskConfiguration != null) {
            if (serviceConfiguration == null) {
              LOG.log(Level.INFO, "Submitting evaluator with context and task");
              ((AllocatedEvaluatorImpl) evaluator).submitContextAndTask(
                  evaluatorConfiguration,
                  contextConfiguration,
                  taskConfiguration);
            } else {
              LOG.log(Level.INFO, "Submitting evaluator with context and service and task");
              ((AllocatedEvaluatorImpl) evaluator).submitContextAndServiceAndTask(
                  evaluatorConfiguration,
                  contextConfiguration,
                  serviceConfiguration,
                  taskConfiguration);
            }
          } else if (contextConfiguration != null) {
            // submit context
            if (serviceConfiguration == null) {
              LOG.log(Level.INFO, "Submitting evaluator with context");
              ((AllocatedEvaluatorImpl) evaluator).submitContext(evaluatorConfiguration, contextConfiguration);
            } else {
              LOG.log(Level.INFO, "Submitting evaluator with context and service");
              ((AllocatedEvaluatorImpl) evaluator)
                  .submitContextAndService(evaluatorConfiguration, contextConfiguration, serviceConfiguration);
            }
          } else if (taskConfiguration != null) {
            // submit task
            if (serviceConfiguration != null) {
              responseObserver.onError(Status.INTERNAL
                  .withDescription("Service must be accompanied by a context configuration")
                  .asRuntimeException());
            } else {
              LOG.log(Level.INFO, "Submitting evaluator with task");
              ((AllocatedEvaluatorImpl) evaluator).submitTask(evaluatorConfiguration, taskConfiguration);
            }
          } else {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Missing check for required evaluator configurations")
                .asRuntimeException());
          }
        }
      }
    }

    @Override
    public void activeContextOp(
        final ActiveContextRequest request,
        final StreamObserver<Void> responseObserver) {
      LOG.log(Level.INFO, "Active context operation {0}", request.getOperationCase());
      synchronized (GRPCDriverService.this) {
        LOG.log(Level.INFO, "i'm in");
        final String contextId = request.getContextId();
        final ActiveContext context = GRPCDriverService.this.activeContextMap.get(contextId);
        if (context == null) {
          LOG.log(Level.SEVERE, "Context does not exist with id {0}", contextId);
          responseObserver.onError(Status.INTERNAL
              .withDescription("Context does not exist with id " + contextId)
              .asRuntimeException());
          return;
        }
        switch (request.getOperationCase()) {
        case CLOSE_CONTEXT:
          if (request.getCloseContext()) {
            try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
              LOG.log(Level.INFO, "closing context {0}", context.getId());
              context.close();
            }
          } else {
            LOG.log(Level.SEVERE, "Close context operation not set to true");
            responseObserver.onError(Status.INTERNAL
                .withDescription("Close context operation not set to true")
                .asRuntimeException());
          }
          break;
        case MESSAGE:
          if (request.getMessage() != null) {
            try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
              LOG.log(Level.INFO, "send message to context {0}", context.getId());
              context.sendMessage(request.getMessage().toByteArray());
            }
          } else {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Empty message on operation send message").asRuntimeException());
          }
          break;
        case NEW_CONTEXT_REQUEST:
          try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
            LOG.log(Level.INFO, "submitting child context to context {0}", context.getId());
            ((EvaluatorContext) context).submitContext(request.getNewContextRequest());
          }
          break;
        case NEW_TASK_REQUEST:
          try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
            LOG.log(Level.INFO, "submitting task to context {0}", context.getId());
            ((EvaluatorContext) context).submitTask(request.getNewTaskRequest());
          }
          break;
        default:
          throw new RuntimeException("Unknown operation " + request.getOperationCase());
        }
      }
    }

    @Override
    public void runningTaskOp(
        final RunningTaskRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
        if (!GRPCDriverService.this.runningTaskMap.containsKey(request.getTaskId())) {
          LOG.log(Level.WARNING, "Unknown task id {0}", request.getTaskId());
          responseObserver.onError(Status.INTERNAL
              .withDescription("Task does not exist with id " + request.getTaskId()).asRuntimeException());
        } else {
          try (ObserverCleanup cleanup = ObserverCleanup.of(responseObserver)) {
            final RunningTask task = GRPCDriverService.this.runningTaskMap.get(request.getTaskId());
            switch (request.getOperation()) {
            case CLOSE:
              LOG.log(Level.INFO, "close task {0}", task.getId());
              if (request.getMessage().isEmpty()) {
                task.close();
              } else {
                task.close(request.getMessage().toByteArray());
              }
              break;
            case SUSPEND:
              LOG.log(Level.INFO, "suspend task {0}", task.getId());
              if (request.getMessage().isEmpty()) {
                task.suspend();
              } else {
                task.suspend(request.getMessage().toByteArray());
              }
              break;
            case SEND_MESSAGE:
              LOG.log(Level.INFO, "send message to task {0}", task.getId());
              task.send(request.getMessage().toByteArray());
              break;
            default:
              throw new RuntimeException("Unknown operation " + request.getOperation());
            }
          }
        }
      }
    }

    private void setEvaluatorProcess(
        final AllocatedEvaluator evaluator,
        final EvaluatorProcess process,
        final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest) {
      if (processRequest.getMemoryMb() > 0) {
        process.setMemory(processRequest.getMemoryMb());
      }
      if (StringUtils.isNotEmpty(processRequest.getConfigurationFileName())) {
        process.setConfigurationFileName(processRequest.getConfigurationFileName());
      }
      if (StringUtils.isNotEmpty(processRequest.getStandardOut())) {
        process.setStandardOut(processRequest.getStandardOut());
      }
      if (StringUtils.isNotEmpty(processRequest.getStandardErr())) {
        process.setStandardErr(processRequest.getStandardErr());
      }
      evaluator.setProcess(process);
    }

    private void setJVMProcess(
        final AllocatedEvaluator evaluator,
        final AllocatedEvaluatorRequest.EvaluatorProcessRequest processRequest) {
      final JVMProcess process = GRPCDriverService.this.jvmProcessFactory.newEvaluatorProcess();
      setEvaluatorProcess(evaluator, process, processRequest);
      if (processRequest.getOptionsCount() > 0) {
        for (final String option : processRequest.getOptionsList()) {
          process.addOption(option);
        }
      }
      evaluator.setProcess(process);
    }
  }
}
