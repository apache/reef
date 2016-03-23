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
package org.apache.reef.runtime.common.driver.context;

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import org.apache.reef.runtime.common.driver.evaluator.pojos.ContextState;
import org.apache.reef.runtime.common.driver.evaluator.pojos.ContextStatusPOJO;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side representation of a Context on an Evaluator.
 */
@DriverSide
@Private
public final class EvaluatorContext implements ActiveContext {

  private static final Logger LOG = Logger.getLogger(EvaluatorContext.class.getName());

  private final String contextIdentifier;
  private final String evaluatorIdentifier;
  private final EvaluatorDescriptor evaluatorDescriptor;

  private final Optional<String> parentID;
  private final ConfigurationSerializer configurationSerializer;
  private final ContextControlHandler contextControlHandler;
  private final ExceptionCodec exceptionCodec;
  private final ContextRepresenters contextRepresenters;

  private boolean isClosed = false;

  public EvaluatorContext(final String contextIdentifier,
                          final String evaluatorIdentifier,
                          final EvaluatorDescriptor evaluatorDescriptor,
                          final Optional<String> parentID,
                          final ConfigurationSerializer configurationSerializer,
                          final ContextControlHandler contextControlHandler,
                          final EvaluatorMessageDispatcher messageDispatcher,
                          final ExceptionCodec exceptionCodec,
                          final ContextRepresenters contextRepresenters) {

    this.contextIdentifier = contextIdentifier;
    this.evaluatorIdentifier = evaluatorIdentifier;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.parentID = parentID;
    this.configurationSerializer = configurationSerializer;
    this.contextControlHandler = contextControlHandler;
    this.exceptionCodec = exceptionCodec;
    this.contextRepresenters = contextRepresenters;

    LOG.log(Level.FINE, "Instantiated 'EvaluatorContext'");
  }

  @Override
  public synchronized void close() {

    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    LOG.log(Level.FINEST, "Submit close context: RunningEvaluator id[{0}] for context id[{1}]",
        new Object[]{getEvaluatorId(), getId()});

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setRemoveContext(
                EvaluatorRuntimeProtocol.RemoveContextProto.newBuilder()
                    .setContextId(getId())
                    .build())
            .build();

    this.contextControlHandler.send(contextControlProto);
    this.isClosed = true;
  }

  @Override
  public synchronized void sendMessage(final byte[] message) {

    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    LOG.log(Level.FINEST, "Send message: RunningEvaluator id[{0}] for context id[{1}]",
        new Object[]{getEvaluatorId(), getId()});

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setContextMessage(EvaluatorRuntimeProtocol.ContextMessageProto.newBuilder()
                .setContextId(this.contextIdentifier)
                .setMessage(ByteString.copyFrom(message))
                .build())
            .build();

    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitTask(final Configuration taskConf) {
    submitTask(this.configurationSerializer.toString(taskConf));
  }

  public synchronized void submitTask(final String taskConf) {
    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    LOG.log(Level.FINEST, "Submit task: RunningEvaluator id[{0}] for context id[{1}]",
        new Object[]{getEvaluatorId(), getId()});

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setStartTask(
                EvaluatorRuntimeProtocol.StartTaskProto.newBuilder()
                    .setContextId(this.contextIdentifier)
                    .setConfiguration(taskConf)
                    .build())
            .build();

    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitContext(final Configuration contextConfiguration) {
    submitContext(this.configurationSerializer.toString(contextConfiguration));
  }

  public synchronized void submitContext(final String contextConf) {
    submitContextAndService(contextConf, Optional.<String>empty());
  }

  @Override
  public synchronized void submitContextAndService(
      final Configuration contextConfiguration, final Configuration serviceConfiguration) {
    submitContextAndService(
        this.configurationSerializer.toString(contextConfiguration),
        this.configurationSerializer.toString(serviceConfiguration));
  }

  public synchronized void submitContextAndService(final String contextConf, final String serviceConf) {
    submitContextAndService(contextConf, Optional.ofNullable(serviceConf));
  }

  public synchronized void submitContextAndService(final String contextConf, final Optional<String> serviceConf) {
    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    EvaluatorRuntimeProtocol.AddContextProto.Builder contextBuilder =
        EvaluatorRuntimeProtocol.AddContextProto.newBuilder()
            .setParentContextId(getId()).setContextConfiguration(contextConf);

    if (serviceConf.isPresent()) {
      contextBuilder = contextBuilder.setServiceConfiguration(serviceConf.get());
    }

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setAddContext(contextBuilder.build())
            .build();

    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorIdentifier;
  }

  @Override
  public Optional<String> getParentId() {
    return this.parentID;
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public String getId() {
    return this.contextIdentifier;
  }

  @Override
  public String toString() {
    return "EvaluatorContext{" +
        "contextIdentifier='" + this.contextIdentifier + '\'' +
        ", evaluatorIdentifier='" + this.evaluatorIdentifier + '\'' +
        ", parentID=" + this.parentID + '}';
  }

  public synchronized ClosedContext getClosedContext(final ActiveContext parentContext) {
    return new ClosedContextImpl(
        parentContext, this.getId(), this.getEvaluatorId(), this.getEvaluatorDescriptor());
  }

  /**
   * @return a FailedContext for the case of an EvaluatorFailure.
   */
  public synchronized FailedContext getFailedContextForEvaluatorFailure() {

    final String id = this.getId();
    final Optional<String> description = Optional.empty();
    final Optional<byte[]> data = Optional.empty();
    final Optional<Throwable> cause = Optional.empty();
    final String message = "Evaluator Failure";

    final Optional<ActiveContext> parentContext = getParentId().isPresent() ?
        Optional.<ActiveContext>of(this.contextRepresenters.getContext(getParentId().get())) :
        Optional.<ActiveContext>empty();

    final String evaluatorID = getEvaluatorId();

    return new FailedContextImpl(
        id, message, description, cause, data, parentContext, this.evaluatorDescriptor, evaluatorID);
  }

  public synchronized FailedContext getFailedContext(
      final ContextStatusPOJO contextStatus) {

    assert ContextState.FAIL == contextStatus.getContextState();

    final String id = this.getId();
    final Optional<String> description = Optional.empty();

    final Optional<byte[]> data = contextStatus.hasError() ?
        Optional.of(contextStatus.getError()) :
        Optional.<byte[]>empty();

    final Optional<Throwable> cause = data.isPresent() ?
        this.exceptionCodec.fromBytes(data) :
        Optional.<Throwable>empty();

    final String message = cause.isPresent() ? cause.get().getMessage() : "No message given";

    final Optional<ActiveContext> parentContext = getParentId().isPresent() ?
        Optional.<ActiveContext>of(this.contextRepresenters.getContext(getParentId().get())) :
        Optional.<ActiveContext>empty();

    final String evaluatorID = getEvaluatorId();

    return new FailedContextImpl(
        id, message, description, cause, data, parentContext, this.evaluatorDescriptor, evaluatorID);
  }

  public synchronized boolean isRootContext() {
    return !this.parentID.isPresent();
  }
}
