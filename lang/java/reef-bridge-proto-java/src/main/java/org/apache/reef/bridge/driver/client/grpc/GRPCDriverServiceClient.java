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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.DriverServiceClient;
import org.apache.reef.bridge.driver.client.JVMClientProcess;
import org.apache.reef.bridge.driver.client.grpc.parameters.DriverRegistrationTimeout;
import org.apache.reef.bridge.driver.client.grpc.parameters.DriverServicePort;
import org.apache.reef.bridge.driver.common.grpc.GRPCUtils;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.bridge.proto.Void;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The client that exposes methods for communicating back to the
 * driver service.
 */
@Private
public final class GRPCDriverServiceClient implements DriverServiceClient {

  private static final Logger LOG = Logger.getLogger(GRPCDriverServiceClient.class.getName());

  private final ExceptionCodec exceptionCodec;

  private final ConfigurationSerializer configurationSerializer;

  private final DriverServiceGrpc.DriverServiceFutureStub serviceStub;

  private final int driverRegistrationTimeout;

  @Inject
  private GRPCDriverServiceClient(
      final ConfigurationSerializer configurationSerializer,
      final ExceptionCodec exceptionCodec,
      @Parameter(DriverServicePort.class) final int driverServicePort,
      @Parameter(DriverRegistrationTimeout.class) final int driverRegistrationTimeout) {
    this.driverRegistrationTimeout = driverRegistrationTimeout;
    this.configurationSerializer = configurationSerializer;
    this.exceptionCodec = exceptionCodec;
    final ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", driverServicePort)
        .usePlaintext()
        .build();
    this.serviceStub = DriverServiceGrpc.newFutureStub(channel);
  }

  public void registerDriverClientService(final String host, final int port) {
    LOG.log(Level.INFO, "Driver client register with driver service on port {0}", port);
    this.serviceStub.registerDriverClient(
        DriverClientRegistration.newBuilder()
            .setHost(host)
            .setPort(port)
            .build());
  }

  @Override
  public void onInitializationException(final Throwable ex) {
    final Future<Void> callComplete = this.serviceStub.registerDriverClient(
        DriverClientRegistration.newBuilder()
            .setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, ex))
            .build());
    try {
      callComplete.get(this.driverRegistrationTimeout, TimeUnit.SECONDS);
    } catch (final ExecutionException | TimeoutException | InterruptedException e) {
      throw new RuntimeException("Cannot register driver client", e);
    }
  }

  @Override
  public void onShutdown() {
    this.serviceStub.shutdown(ShutdownRequest.newBuilder().build());
  }

  @Override
  public void onShutdown(final Throwable ex) {
    this.serviceStub.shutdown(ShutdownRequest.newBuilder()
        .setException(GRPCUtils.createExceptionInfo(this.exceptionCodec, ex))
        .build());
  }

  @Override
  public void onSetAlarm(final String alarmId, final int timeoutMS) {
    this.serviceStub.setAlarm(
        AlarmRequest.newBuilder()
            .setAlarmId(alarmId)
            .setTimeoutMs(timeoutMS)
            .build());
  }

  @Override
  public void onEvaluatorRequest(final EvaluatorRequest evaluatorRequest) {
    this.serviceStub.requestResources(
        ResourceRequest.newBuilder()
            .setCores(evaluatorRequest.getNumberOfCores())
            .setMemorySize(evaluatorRequest.getMegaBytes())
            .setRelaxLocality(evaluatorRequest.getRelaxLocality())
            .setResourceCount(evaluatorRequest.getNumber())
            .setRuntimeName(evaluatorRequest.getRuntimeName())
            .addAllRackNameList(evaluatorRequest.getRackNames())
            .addAllNodeNameList(evaluatorRequest.getNodeNames())
            .build());
  }

  @Override
  public void onEvaluatorClose(final String evalautorId) {
    this.serviceStub.allocatedEvaluatorOp(
        AllocatedEvaluatorRequest.newBuilder()
            .setEvaluatorId(evalautorId)
            .setCloseEvaluator(true)
            .build());
  }

  @Override
  public void onEvaluatorSubmit(
      final String evaluatorId,
      final Optional<Configuration> contextConfiguration,
      final Optional<Configuration> taskConfiguration,
      final Optional<JVMClientProcess> evaluatorProcess,
      final List<File> addFileList,
      final List<File> addLibraryList) {
    final AllocatedEvaluatorRequest.Builder builder =
        AllocatedEvaluatorRequest.newBuilder().setEvaluatorId(evaluatorId);
    for (final File file : addFileList) {
      builder.addAddFiles(file.getAbsolutePath());
    }
    for (final File file : addLibraryList) {
      builder.addAddLibraries(file.getAbsolutePath());
    }
    if (evaluatorProcess.isPresent()) {
      final JVMClientProcess rawEP = evaluatorProcess.get();
      builder.setSetProcess(
          AllocatedEvaluatorRequest.EvaluatorProcessRequest.newBuilder()
              .setConfigurationFileName(rawEP.getConfigurationFileName())
              .setMemoryMb(rawEP.getMemory())
              .setStandardOut(rawEP.getStandardOut())
              .setStandardErr(rawEP.getStandardErr())
              .addAllOptions(rawEP.getOptions())
              .build());
    }
    if (contextConfiguration.isPresent()) {
      builder.setContextConfiguration(
          this.configurationSerializer.toString(contextConfiguration.get()));
    } else {
      builder.setContextConfiguration(this.configurationSerializer.toString(ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "context-" + evaluatorId)
          .build()));
    }
    if (taskConfiguration.isPresent()) {
      builder.setTaskConfiguration(
          this.configurationSerializer.toString(taskConfiguration.get()));
    }
    this.serviceStub.allocatedEvaluatorOp(builder.build());
  }

  // Context Operations

  @Override
  public void onContextClose(final String contextId) {
    this.serviceStub.activeContextOp(
        ActiveContextRequest.newBuilder()
            .setContextId(contextId)
            .setCloseContext(true)
            .build());
  }

  @Override
  public void onContextSubmitContext(
      final String contextId,
      final Configuration contextConfiguration) {
    this.serviceStub.activeContextOp(
        ActiveContextRequest.newBuilder()
            .setContextId(contextId)
            .setNewContextRequest(this.configurationSerializer.toString(contextConfiguration))
            .build());
  }

  @Override
  public void onContextSubmitTask(
      final String contextId,
      final Configuration taskConfiguration) {
    this.serviceStub.activeContextOp(
        ActiveContextRequest.newBuilder()
            .setContextId(contextId)
            .setNewTaskRequest(this.configurationSerializer.toString(taskConfiguration))
            .build());
  }

  @Override
  public void onContextMessage(final String contextId, final byte[] message) {
    this.serviceStub.activeContextOp(
        ActiveContextRequest.newBuilder()
            .setContextId(contextId)
            .setMessage(ByteString.copyFrom(message))
            .build());
  }

  // Task operations

  @Override
  public void onTaskClose(final String taskId, final Optional<byte[]> message) {
    this.taskOp(RunningTaskRequest.Operation.CLOSE, taskId, message.orElse(null));
  }

  @Override
  public void onTaskMessage(final String taskId, final byte[] message) {
    this.taskOp(RunningTaskRequest.Operation.SEND_MESSAGE, taskId, message);
  }

  @Override
  public void onSuspendTask(final String taskId, final Optional<byte[]> message) {
    this.taskOp(RunningTaskRequest.Operation.SUSPEND, taskId, message.orElse(null));
  }

  private void taskOp(final RunningTaskRequest.Operation op, final String taskId, final byte[] message) {
    final RunningTaskRequest.Builder request = RunningTaskRequest.newBuilder().setTaskId(taskId).setOperation(op);
    if (message != null && message.length > 0) {
      request.setMessage(ByteString.copyFrom(message));
    }
    this.serviceStub.runningTaskOp(request.build());
  }
}
