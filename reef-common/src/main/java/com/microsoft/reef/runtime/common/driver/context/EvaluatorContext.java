/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.context;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorMessageDispatcher;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.ConfigurationSerializer;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side representation of a Context on an Evaluator.
 */
@DriverSide
@Private
public final class EvaluatorContext implements ActiveContext {

  private final static Logger LOG = Logger.getLogger(EvaluatorContext.class.getName());

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
                    .setConfiguration(this.configurationSerializer.toString(taskConf))
                    .build())
            .build();

    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitContext(final Configuration contextConfiguration) {

    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    LOG.log(Level.FINEST, "Submit new context: RunningEvaluator id[{0}] for context id[{1}]",
        new Object[]{getEvaluatorId(), getId()});

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setAddContext(
                EvaluatorRuntimeProtocol.AddContextProto.newBuilder()
                    .setParentContextId(getId())
                    .setContextConfiguration(this.configurationSerializer.toString(contextConfiguration))
                    .build())
            .build();

    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitContextAndService(
      final Configuration contextConfiguration, final Configuration serviceConfiguration) {

    if (this.isClosed) {
      throw new RuntimeException("Active context already closed");
    }

    LOG.log(Level.FINEST, "Submit new context: RunningEvaluator id[{0}] for context id[{1}]",
        new Object[]{getEvaluatorId(), getId()});

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setAddContext(
                EvaluatorRuntimeProtocol.AddContextProto.newBuilder()
                    .setParentContextId(getId())
                    .setContextConfiguration(this.configurationSerializer.toString(contextConfiguration))
                    .setServiceConfiguration(this.configurationSerializer.toString(serviceConfiguration))
                    .build())
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

  public synchronized final ClosedContext getClosedContext(final ActiveContext parentContext) {
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

    final EvaluatorDescriptor evaluatorDescriptor = getEvaluatorDescriptor();
    final String evaluatorID = getEvaluatorId();

    return new FailedContextImpl(
        id, message, description, cause, data, parentContext, evaluatorDescriptor, evaluatorID);
  }

  public synchronized FailedContext getFailedContext(
      final ReefServiceProtos.ContextStatusProto contextStatusProto) {

    assert (ReefServiceProtos.ContextStatusProto.State.FAIL == contextStatusProto.getContextState());

    final String id = this.getId();
    final Optional<String> description = Optional.empty();

    final Optional<byte[]> data = contextStatusProto.hasError() ?
        Optional.of(contextStatusProto.getError().toByteArray()) :
        Optional.<byte[]>empty();

    final Optional<Throwable> cause = data.isPresent() ?
        this.exceptionCodec.fromBytes(data) :
        Optional.<Throwable>empty();

    final String message = cause.isPresent() ? cause.get().getMessage() : "No message given";

    final Optional<ActiveContext> parentContext = getParentId().isPresent() ?
        Optional.<ActiveContext>of(this.contextRepresenters.getContext(getParentId().get())) :
        Optional.<ActiveContext>empty();

    final EvaluatorDescriptor evaluatorDescriptor = getEvaluatorDescriptor();
    final String evaluatorID = getEvaluatorId();

    return new FailedContextImpl(
        id, message, description, cause, data, parentContext, evaluatorDescriptor, evaluatorID);
  }

  public synchronized boolean isRootContext() {
    return !this.parentID.isPresent();
  }
}
