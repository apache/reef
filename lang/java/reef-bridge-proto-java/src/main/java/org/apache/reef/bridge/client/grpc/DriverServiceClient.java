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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.client.IDriverServiceClient;
import org.apache.reef.bridge.client.grpc.parameters.DriverServicePort;
import org.apache.reef.bridge.proto.*;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * The client that exposes methods for communicating back to the
 * driver service.
 */
@Private
public final class DriverServiceClient implements IDriverServiceClient {

  /** Used for the evaluator configuration, which is not needed in Java. */
  private static final Configuration EMPTY_CONF =
      Tang.Factory.getTang().newConfigurationBuilder().build();

  private final ConfigurationSerializer configurationSerializer;

  private final DriverServiceGrpc.DriverServiceBlockingStub serviceStub;

  @Inject
  private DriverServiceClient(
      final ConfigurationSerializer configurationSerializer,
      @Parameter(DriverServicePort.class) final Integer driverServicePort) {
    this.configurationSerializer = configurationSerializer;
    final ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", driverServicePort)
        .usePlaintext(true)
        .build();
    this.serviceStub = DriverServiceGrpc.newBlockingStub(channel);
  }

  public void registerDriverClientService(final String host, final int port) {
    this.serviceStub.registerDriverClient(
        DriverClientRegistration.newBuilder()
            .setHost(host)
            .setPort(port)
            .build());
  }

  @Override
  public void onShutdown() {
    this.serviceStub.shutdown(ShutdownRequest.newBuilder().build());
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
  public void onEvalautorClose(final String evalautorId) {
    this.serviceStub.allocatedEvaluatorOp(
        AllocatedEvaluatorRequest.newBuilder()
            .setEvaluatorId(evalautorId)
            .setCloseEvaluator(true)
            .build());
  }

  @Override
  public void onEvaluatorSubmit(
      final String evaluatorId,
      final Configuration contextConfiguration,
      final Optional<Configuration> taskConfiguration) {
    this.serviceStub.allocatedEvaluatorOp(
        AllocatedEvaluatorRequest.newBuilder()
            .setEvaluatorId(evaluatorId)
            .setEvaluatorConfiguration(this.configurationSerializer.toString(EMPTY_CONF))
            .setContextConfiguration(this.configurationSerializer.toString(contextConfiguration))
            .setTaskConfiguration(taskConfiguration.isPresent() ?
                this.configurationSerializer.toString(taskConfiguration.get()) : null)
            .build());
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
    this.serviceStub.runningTaskOp(RunningTaskRequest.newBuilder()
        .setTaskId(taskId)
        .setCloseTask(true)
        .setMessage(message.isPresent() ? ByteString.copyFrom(message.get()) : null)
        .build());
  }

  @Override
  public void onTaskMessage(final String taskId, final byte[] message) {
    this.serviceStub.runningTaskOp(RunningTaskRequest.newBuilder()
        .setTaskId(taskId)
        .setMessage(ByteString.copyFrom(message))
        .build());
  }
}
