/**
 * Copyright (C) 2013 Microsoft Corporation
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
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.ConfigurationSerializer;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class EvaluatorContext implements ActiveContext {

  private final static Logger LOG = Logger.getLogger(ActiveContext.class.getName());

  private final String contextIdentifier;
  private final String evaluatorIdentifier;
  private final EvaluatorDescriptor evaluatorDescriptor;


  private final Optional<String> parentID;
  private final ConfigurationSerializer configurationSerializer;
  private final ContextControlHandler contextControlHandler;

  private boolean closed = false;

  public EvaluatorContext(final String contextIdentifier,
                          final String evaluatorIdentifier,
                          final EvaluatorDescriptor evaluatorDescriptor,
                          final Optional<String> parentID,
                          final ConfigurationSerializer configurationSerializer,
                          final ContextControlHandler contextControlHandler) {
    this.contextIdentifier = contextIdentifier;
    this.evaluatorIdentifier = evaluatorIdentifier;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.parentID = parentID;
    this.configurationSerializer = configurationSerializer;
    this.contextControlHandler = contextControlHandler;
  }

  @Override
  public synchronized void close() {
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Submit close context: RunningEvaluator id[" + getEvaluatorId() + "] for context id[" + getId() + "]");

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setRemoveContext(
                EvaluatorRuntimeProtocol.RemoveContextProto.newBuilder()
                    .setContextId(getId())
                    .build()
            ).build();
    this.contextControlHandler.send(contextControlProto);
    this.closed = true;
  }

  @Override
  public synchronized void sendMessage(final byte[] message) {
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Send message: RunningEvaluator id[" + getEvaluatorId() + "] context id[" + getId() + "]");

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
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Submit task: RunningEvaluator id[" + getEvaluatorId() + "] context id[" + getId() + "]");

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setStartTask(
                EvaluatorRuntimeProtocol.StartTaskProto.newBuilder()
                    .setContextId(this.contextIdentifier)
                    .setConfiguration(this.configurationSerializer.toString(taskConf))
                    .build()
            ).build();
    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitContext(final Configuration contextConfiguration) {
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Submit new context: RunningEvaluator id[" + getEvaluatorId() + "] parent context id[" + getId() + "]");

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setAddContext(
                EvaluatorRuntimeProtocol.AddContextProto.newBuilder()
                    .setParentContextId(getId())
                    .setContextConfiguration(this.configurationSerializer.toString(contextConfiguration))
                    .build()
            ).build();
    this.contextControlHandler.send(contextControlProto);
  }

  @Override
  public synchronized void submitContextAndService(final Configuration contextConfiguration, final Configuration serviceConfiguration) {
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Submit new context: RunningEvaluator id[" + getEvaluatorId() + "] parent context id[" + getId() + "]");

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setAddContext(
                EvaluatorRuntimeProtocol.AddContextProto.newBuilder()
                    .setParentContextId(getId())
                    .setContextConfiguration(this.configurationSerializer.toString(contextConfiguration))
                    .setServiceConfiguration(this.configurationSerializer.toString(serviceConfiguration))
                    .build()
            ).build();
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


  public final ClosedContext getClosedContext(final ActiveContext parentContext) {
    return new ClosedContextImpl(parentContext, this.getId(), this.getEvaluatorId(), this.getEvaluatorDescriptor());
  }

  public final FailedContext getFailedContext(
      final Optional<ActiveContext> parentContext, final Exception reason) {
    return new FailedContextImpl(reason, this.getId(), parentContext, this.getEvaluatorId(), this.getEvaluatorDescriptor());
  }
}
