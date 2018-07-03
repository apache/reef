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
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.common.grpc.GRPCUtils;
import org.apache.reef.bridge.driver.common.grpc.ObserverCleanup;
import org.apache.reef.bridge.driver.service.DriverClientException;
import org.apache.reef.bridge.driver.service.DriverService;
import org.apache.reef.bridge.service.parameters.DriverClientCommand;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.context.*;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;
import org.apache.reef.runtime.common.driver.evaluator.AllocatedEvaluatorImpl;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
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
public final class GRPCDriverService implements DriverService {
  private static final Logger LOG = Logger.getLogger(GRPCDriverService.class.getName());

  private static final Void VOID = Void.newBuilder().build();

  private Process driverProcess;

  private enum StreamType { STDOUT, STDERR }

  private Server server;

  private DriverClientGrpc.DriverClientFutureStub clientStub;

  private final Clock clock;

  private final REEFFileNames reefFileNames;

  private final ExceptionCodec exceptionCodec;

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
      final REEFFileNames reefFileNames,
      final EvaluatorRequestor evaluatorRequestor,
      final ConfigurationSerializer configurationSerializer,
      final JVMProcessFactory jvmProcessFactory,
      final CLRProcessFactory clrProcessFactory,
      final TcpPortProvider tcpPortProvider,
      final ExceptionCodec exceptionCodec,
      @Parameter(DriverClientCommand.class) final String driverClientCommand) {
    this.clock = clock;
    this.reefFileNames = reefFileNames;
    this.exceptionCodec = exceptionCodec;
    this.configurationSerializer = configurationSerializer;
    this.jvmProcessFactory = jvmProcessFactory;
    this.clrProcessFactory = clrProcessFactory;
    this.evaluatorRequestor = evaluatorRequestor;
    this.driverClientCommand = driverClientCommand;
    this.tcpPortProvider = tcpPortProvider;
  }

  private void start() throws IOException, InterruptedException {
    for (final int port : this.tcpPortProvider) {
      try {
        this.server = ServerBuilder.forPort(port)
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
      int attempts = 30; // give some time
      /* wait for driver client process to register
       * Note: attempts and wait time have been given reasonable hardcoded values for a driver
       * client to register with the driver service (us). Making these values configurable would
       * require additions to the ClientProtocol buffer such that they can be passed to the
       * GRPCDriverServiceConfigurationProvider and bound to the appropriate NamedParameters. It
       * is the opinion at the time of this writing that a driver client should be able to register
       * within 10 seconds.
       */
      while (attempts-- > 0 && this.clientStub == null && driverProcessIsAlive()) {
        LOG.log(Level.INFO, "waiting for driver process to register");
        this.wait(1000); // a second
      }
    }
    if (driverProcessIsAlive()) {
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
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
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
    if (this.clientStub != null) {
      try {
        final IdleStatus idleStatus = this.clientStub.idlenessCheckHandler(VOID).get();
        LOG.log(Level.INFO, "is idle: {0}", idleStatus.getIsIdle());
        return new IdleMessage(
            componentName,
            idleStatus.getReason(),
            idleStatus.getIsIdle());
      } catch (final ExecutionException | InterruptedException e) {
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
    } catch (final IOException | InterruptedException e) {
      stop(e);
    }
  }

  @Override
  public void stopHandler(final StopTime stopTime) {
    synchronized (this) {
      if (clientStub != null) {
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
              throw new RuntimeException(error.getMessage() != null ? error.getMessage() : error.getName());
            }
          }
        } catch (final TimeoutException e) {
          throw new RuntimeException("stop handler timed out", e);
        } catch (final InterruptedException | ExecutionException e) {
          throw new RuntimeException("error in stop handler", e);
        } finally {
          stop();
        }
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
              .setDescriptorInfo(
                  GRPCUtils.toEvaluatorDescriptorInfo(eval.getEvaluatorDescriptor()))
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
      this.clientStub.activeContextHandler(GRPCUtils.toContextInfo(context));
    }
  }

  @Override
  public void closedContextHandler(final ClosedContext context) {
    synchronized (this) {
      this.activeContextMap.remove(context.getId());
      this.clientStub.closedContextHandler(GRPCUtils.toContextInfo(context));
    }
  }

  @Override
  public void failedContextHandler(final FailedContext context) {
    synchronized (this) {
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
              .setContext(GRPCUtils.toContextInfo(context))
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
      final TaskInfo.Builder taskInfoBuilder = TaskInfo.newBuilder()
          .setTaskId(task.getId());
      if (task.getActiveContext().isPresent()) {
        taskInfoBuilder.setContext(GRPCUtils.toContextInfo(task.getActiveContext().get()));
      }
      if (task.getReason().isPresent()) {
        taskInfoBuilder.setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, task.getReason().get()));
      } else if (task.getData().isPresent()) {
        final Throwable reason = task.asError();
        taskInfoBuilder.setException(ExceptionInfo.newBuilder()
            .setName(reason.toString())
            .setMessage(task.getMessage() != null ? task.getMessage() : "")
            .setData(ByteString.copyFrom(task.getData().get()))
            .build());
      } else {
        taskInfoBuilder.setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, task.asError()));
      }
      this.runningTaskMap.remove(task.getId());
      this.clientStub.failedTaskHandler(taskInfoBuilder.build());
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
              .setContext(GRPCUtils.toContextInfo(task.getActiveContext()))
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
              .setContext(GRPCUtils.toContextInfo(task.getActiveContext()))
              .setResult(task.get() == null || task.get().length == 0 ?
                  null : ByteString.copyFrom(task.get()))
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
    } catch (final InterruptedException | IOException e) {
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
              .setContext(GRPCUtils.toContextInfo(context))
              .build());
    }
  }

  @Override
  public void restartActiveContext(final ActiveContext context) {
    synchronized (this) {
      this.activeContextMap.put(context.getId(), context);
      this.clientStub.driverRestartActiveContextHandler(
          GRPCUtils.toContextInfo(context));
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

  private Optional<Throwable> parseException(final ExceptionInfo info) {
    if (info.getData() == null || info.getData().isEmpty()) {
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
      try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
        if (request.hasException()) {
          LOG.log(Level.SEVERE, "Driver client initialization exception");
          final Optional<Throwable> ex = parseException(request.getException());
          if (ex.isPresent()) {
            GRPCDriverService.this.clock.stop(ex.get());
          } else {
            GRPCDriverService.this.clock.stop(new RuntimeException(
                request.getException().getMessage() == null ?
                    request.getException().getName() :
                    request.getException().getMessage()
            ));
          }
        } else {
          final ManagedChannel channel = ManagedChannelBuilder
              .forAddress(request.getHost(), request.getPort())
              .usePlaintext()
              .build();
          synchronized (GRPCDriverService.this) {
            GRPCDriverService.this.clientStub = DriverClientGrpc.newFutureStub(channel);
            GRPCDriverService.this.notifyAll();
          }
          LOG.log(Level.INFO, "Driver has registered on port {0}", request.getPort());
        }
      }
    }

    @Override
    public void requestResources(
        final ResourceRequest request,
        final StreamObserver<Void> responseObserver) {
      try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
      try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
        LOG.log(Level.INFO, "driver shutdown");
        if (request.hasException()) {
          final Optional<Throwable> exception = parseException(request.getException());
          if (exception.isPresent()) {
            LOG.log(Level.INFO, "driver exception", exception.get());
            GRPCDriverService.this.clock.stop(exception.get());
          } else {
            // exception that cannot be parsed in java
            GRPCDriverService.this.clock.stop(
                new DriverClientException(request.getException().getMessage()));
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
      try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
      try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
              for (final String file : request.getAddFilesList()) {
                evaluator.addFile(new File(file));
              }
              for (final String library : request.getAddLibrariesList()) {
                evaluator.addLibrary(new File(library));
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
                if (StringUtils.isNotEmpty(request.getContextConfiguration()) &&
                    StringUtils.isNotEmpty(request.getTaskConfiguration())) {
                  // submit context and task
                  try {
                    evaluator.submitContextAndTask(
                        configurationSerializer.fromString(request.getContextConfiguration()),
                        configurationSerializer.fromString(request.getTaskConfiguration()));
                  } catch (final IOException e) {
                    throw new RuntimeException("error submitting task and context", e);
                  }
                } else if (StringUtils.isNotEmpty(request.getContextConfiguration())) {
                  // submit context
                  try {
                    evaluator.submitContext(configurationSerializer.fromString(request.getContextConfiguration()));
                  } catch (final IOException e) {
                    throw new RuntimeException("error submitting context", e);
                  }
                } else if (StringUtils.isNotEmpty(request.getTaskConfiguration())) {
                  // submit task
                  try {
                    evaluator.submitTask(configurationSerializer.fromString(request.getTaskConfiguration()));
                  } catch (final IOException e) {
                    throw new RuntimeException("error submitting task", e);
                  }
                } else {
                  throw new RuntimeException("Missing check for required evaluator configurations");
                }
              } else {
                if (StringUtils.isNotEmpty(request.getContextConfiguration()) &&
                    StringUtils.isNotEmpty(request.getTaskConfiguration())) {
                  // submit context and task
                  ((AllocatedEvaluatorImpl) evaluator).submitContextAndTask(
                      request.getEvaluatorConfiguration(),
                      request.getContextConfiguration(),
                      request.getTaskConfiguration());
                } else if (StringUtils.isNotEmpty(request.getContextConfiguration())) {
                  // submit context
                  ((AllocatedEvaluatorImpl) evaluator).submitContext(
                      request.getEvaluatorConfiguration(),
                      request.getContextConfiguration());
                } else if (StringUtils.isNotEmpty(request.getTaskConfiguration())) {
                  // submit task
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
      }
    }

    @Override
    public void activeContextOp(
        final ActiveContextRequest request,
        final StreamObserver<Void> responseObserver) {
      synchronized (GRPCDriverService.this) {
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
            try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
            try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
              LOG.log(Level.INFO, "send message to context {0}", context.getId());
              context.sendMessage(request.getMessage().toByteArray());
            }
          } else {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Empty message on operation send message").asRuntimeException());
          }
          break;
        case NEW_CONTEXT_REQUEST:
          try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
            LOG.log(Level.INFO, "submitting child context to context {0}", context.getId());
            ((EvaluatorContext) context).submitContext(request.getNewContextRequest());
          }
          break;
        case NEW_TASK_REQUEST:
          try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
          try (final ObserverCleanup _cleanup = ObserverCleanup.of(responseObserver)) {
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
