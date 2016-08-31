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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.api.ResourceManagerStartHandler;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorHeartbeatHandler;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorResourceManagerErrorHandler;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceManagerStatus;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The RuntimeStart handler of the Driver.
 * <p>
 * This instantiates the DriverSingletons upon construction. Upon onNext(),
 * it sets the resource manager status and wires up the remote event handler
 * connections to the client and the evaluators.
 */
final class DriverRuntimeStartHandler implements EventHandler<RuntimeStart> {

  private static final Logger LOG = Logger.getLogger(DriverRuntimeStartHandler.class.getName());

  private final RemoteManager remoteManager;
  private final EvaluatorResourceManagerErrorHandler evaluatorResourceManagerErrorHandler;
  private final EvaluatorHeartbeatHandler evaluatorHeartbeatHandler;
  private final ResourceManagerStatus resourceManagerStatus;
  private final ResourceManagerStartHandler resourceManagerStartHandler;
  private final DriverStatusManager driverStatusManager;

  /**
   * @param singletons                           the objects we want to be Singletons in the Driver
   * @param remoteManager                        the remoteManager in the Driver.
   * @param evaluatorResourceManagerErrorHandler This will be wired up to the remoteManager on onNext()
   * @param evaluatorHeartbeatHandler            This will be wired up to the remoteManager on onNext()
   * @param resourceManagerStartHandler          This will initialize the resource manager
   * @param resourceManagerStatus                will be set to RUNNING in onNext()
   * @param driverStatusManager                  will be set to RUNNING in onNext()
   */
  @Inject
  private DriverRuntimeStartHandler(
      final DriverSingletons singletons,
      final RemoteManager remoteManager,
      final EvaluatorResourceManagerErrorHandler evaluatorResourceManagerErrorHandler,
      final EvaluatorHeartbeatHandler evaluatorHeartbeatHandler,
      final ResourceManagerStatus resourceManagerStatus,
      final ResourceManagerStartHandler resourceManagerStartHandler,
      final DriverStatusManager driverStatusManager) {

    this.remoteManager = remoteManager;
    this.evaluatorResourceManagerErrorHandler = evaluatorResourceManagerErrorHandler;
    this.evaluatorHeartbeatHandler = evaluatorHeartbeatHandler;
    this.resourceManagerStatus = resourceManagerStatus;
    this.resourceManagerStartHandler = resourceManagerStartHandler;
    this.driverStatusManager = driverStatusManager;
  }

  /**
   * This method is called on start of the REEF Driver runtime event loop.
   * It contains startup logic for REEF Driver that is independent from a
   * runtime framework (e.g. Mesos, YARN, Local, etc).
   * Platform-specific logic is then handled in ResourceManagerStartHandler.
   * @param runtimeStart An event that signals start of the Driver runtime.
   * Contains a timestamp and can be pretty printed.
   */
  @Override
  public synchronized void onNext(final RuntimeStart runtimeStart) {

    LOG.log(Level.FINEST, "RuntimeStart: {0}", runtimeStart);

    // Register for heartbeats and error messages from the Evaluators.
    this.remoteManager.registerHandler(
        EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.class,
        this.evaluatorHeartbeatHandler);

    this.remoteManager.registerHandler(
        ReefServiceProtos.RuntimeErrorProto.class,
        this.evaluatorResourceManagerErrorHandler);

    this.resourceManagerStatus.setRunning();
    this.driverStatusManager.onRunning();

    // Forward start event to the runtime-specific handler (e.g. YARN, Local, etc.)
    this.resourceManagerStartHandler.onNext(runtimeStart);

    LOG.log(Level.FINEST, "DriverRuntimeStartHandler complete.");
  }
}
