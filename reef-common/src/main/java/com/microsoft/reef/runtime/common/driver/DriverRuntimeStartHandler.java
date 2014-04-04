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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorHeartbeatHandler;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorRuntimeErrorHandler;
import com.microsoft.reef.runtime.common.driver.runtime.RuntimeStatusManager;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The RuntimeStart handler of the Driver.
 */
final class DriverRuntimeStartHandler implements EventHandler<RuntimeStart> {
  private static final Logger LOG = Logger.getLogger(DriverRuntimeStartHandler.class.getName());
  private final RemoteManager remoteManager;
  private final EvaluatorRuntimeErrorHandler evaluatorRuntimeErrorHandler;
  private final EvaluatorHeartbeatHandler evaluatorHeartbeatHandler;
  private final RuntimeStatusManager runtimeStatusManager;

  /**
   * @param singletons                   the objects we want to be Singletons in the Driver
   * @param remoteManager                the remoteManager in the Driver.
   * @param evaluatorRuntimeErrorHandler This will be wired up to the remoteManager on onNext()
   * @param evaluatorHeartbeatHandler    This will be wired up to the remoteManager on onNext()
   * @param runtimeStatusManager         will be set to RUNNING in onNext()
   */
  @Inject
  private DriverRuntimeStartHandler(final DriverSingletons singletons,
                                    final RemoteManager remoteManager,
                                    final EvaluatorRuntimeErrorHandler evaluatorRuntimeErrorHandler,
                                    final EvaluatorHeartbeatHandler evaluatorHeartbeatHandler,
                                    final RuntimeStatusManager runtimeStatusManager) {
    this.remoteManager = remoteManager;
    this.evaluatorRuntimeErrorHandler = evaluatorRuntimeErrorHandler;
    this.evaluatorHeartbeatHandler = evaluatorHeartbeatHandler;
    this.runtimeStatusManager = runtimeStatusManager;
  }

  @Override
  public synchronized void onNext(final RuntimeStart runtimeStart) {
    LOG.log(Level.FINEST, "RuntimeStart: {0}", runtimeStart);
    this.remoteManager.registerHandler(EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.class, evaluatorHeartbeatHandler);
    this.remoteManager.registerHandler(ReefServiceProtos.RuntimeErrorProto.class, evaluatorRuntimeErrorHandler);
    this.runtimeStatusManager.setRunning();
    LOG.log(Level.FINEST, "DriverRuntimeStartHandler complete.");
  }
}
