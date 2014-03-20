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
package com.microsoft.reef.runtime.common.driver;

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

  private final String identifier;

  private final Optional<String> parentID;

  private final EvaluatorManager evaluatorManager;
  private final ConfigurationSerializer configurationSerializer;

  private boolean closed = false;

  public EvaluatorContext(final EvaluatorManager evaluatorManager,
                          final String identifier, Optional<String> parentID,
                          final ConfigurationSerializer configurationSerializer) {
    this.identifier = identifier;
    this.parentID = parentID;
    this.evaluatorManager = evaluatorManager;
    this.configurationSerializer = configurationSerializer;
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
    this.evaluatorManager.handle(contextControlProto);
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
                .setContextId(this.identifier)
                .setMessage(ByteString.copyFrom(message))
                .build())
            .build();
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public synchronized void submitTask(Configuration taskConf) {
    if (this.closed) {
      throw new RuntimeException("Active context already closed");
    }
    LOG.log(Level.FINEST, "Submit task: RunningEvaluator id[" + getEvaluatorId() + "] context id[" + getId() + "]");

    final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto =
        EvaluatorRuntimeProtocol.ContextControlProto.newBuilder()
            .setStartTask(
                EvaluatorRuntimeProtocol.StartTaskProto.newBuilder()
                    .setContextId(this.identifier)
                    .setConfiguration(this.configurationSerializer.toString(taskConf))
                    .build()
            ).build();
    this.evaluatorManager.handle(contextControlProto);
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
    this.evaluatorManager.handle(contextControlProto);

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
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorManager.getId();
  }

  @Override
  public Optional<String> getParentId() {
    return this.parentID;
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorManager.getEvaluatorDescriptor();
  }

  @Override
  public String getId() {
    return this.identifier;
  }

  final ClosedContext getClosedContext(final ActiveContext parentContext) {
    return new ClosedContext() {

      @Override
      public ActiveContext getParentContext() {
        return parentContext;
      }

      @Override
      public String getId() {
        return EvaluatorContext.this.getId();
      }

      @Override
      public String getEvaluatorId() {
        return EvaluatorContext.this.getEvaluatorId();
      }

      @Override
      public Optional<String> getParentId() {
        return EvaluatorContext.this.getParentId();
      }

      @Override
      public EvaluatorDescriptor getEvaluatorDescriptor() {
        return EvaluatorContext.this.evaluatorManager.getEvaluatorDescriptor();
      }
    };
  }

  final FailedContext getFailedContext(
      final Optional<ActiveContext> parentContext, final Exception reason) {

    return new FailedContext(EvaluatorContext.this.getId(), reason) {

      @Override
      public Optional<ActiveContext> getParentContext() {
        return parentContext;
      }

      @Override
      public String getEvaluatorId() {
        return EvaluatorContext.this.getEvaluatorId();
      }

      @Override
      public Optional<String> getParentId() {
        return EvaluatorContext.this.getParentId();
      }

      @Override
      public EvaluatorDescriptor getEvaluatorDescriptor() {
        return EvaluatorContext.this.evaluatorManager.getEvaluatorDescriptor();
      }
    };
  }
}
