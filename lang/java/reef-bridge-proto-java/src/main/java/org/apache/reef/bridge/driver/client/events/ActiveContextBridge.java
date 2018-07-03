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

package org.apache.reef.bridge.driver.client.events;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.DriverServiceClient;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Active context bridge.
 */
@Private
public final class ActiveContextBridge implements ActiveContext {

  private static final Logger LOG = Logger.getLogger(ActiveContextBridge.class.getName());

  private final DriverServiceClient driverServiceClient;

  private final String contextId;

  private final Optional<String> parentId;

  private final String evaluatorId;

  private final EvaluatorDescriptor evaluatorDescriptor;

  public ActiveContextBridge(
      final DriverServiceClient driverServiceClient,
      final String contextId,
      final Optional<String> parentId,
      final String evaluatorId,
      final EvaluatorDescriptor evaluatorDescriptor) {
    this.driverServiceClient = driverServiceClient;
    this.contextId = contextId;
    this.parentId = parentId;
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
  }

  @Override
  public void close() {
    LOG.log(Level.INFO, "closing context {0}", this.contextId);
    this.driverServiceClient.onContextClose(this.contextId);
  }

  @Override
  public void submitTask(final Configuration taskConf) {
    LOG.log(Level.INFO, "submitting task via context {0}", this.contextId);
    this.driverServiceClient.onContextSubmitTask(this.contextId, taskConf);
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {
    LOG.log(Level.INFO, "submitting child context via context {0}", this.contextId);
    this.driverServiceClient.onContextSubmitContext(this.contextId, contextConfiguration);
  }

  @Override
  public void submitContextAndService(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration) {
    throw new UnsupportedOperationException("Service not supported");
  }

  @Override
  public void sendMessage(final byte[] message) {
    LOG.log(Level.INFO, "sending message to context {0}", this.contextId);
    this.driverServiceClient.onContextMessage(this.contextId, message);
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorId;
  }

  @Override
  public Optional<String> getParentId() {
    return this.parentId;
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public String getId() {
    return this.contextId;
  }
}
