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
package org.apache.reef.runtime.local.process;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.local.driver.ResourceManager;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * a RunnableProcessObserver that relies events to REEF's ResourceStatusHandler.
 */
@ThreadSafe
public final class ReefRunnableProcessObserver implements RunnableProcessObserver {
  private static final Logger LOG = Logger.getLogger(ReefRunnableProcessObserver.class.getName());

  private final EventHandler<ResourceStatusEvent> resourceStatusHandler;
  private final InjectionFuture<ResourceManager> resourceManager;

  /**
   * @param resourceStatusHandler the event handler to inform of resource changes.
   */
  @Inject
  public ReefRunnableProcessObserver(@Parameter(RuntimeParameters.ResourceStatusHandler.class) final 
                                     EventHandler<ResourceStatusEvent> resourceStatusHandler,
                                     final InjectionFuture<ResourceManager> resourceManager) {
    this.resourceStatusHandler = resourceStatusHandler;
    this.resourceManager = resourceManager;
  }

  @Override
  public void onProcessStarted(final String processId) {
    this.onResourceStatus(
        ResourceStatusEventImpl.newBuilder()
            .setIdentifier(processId)
            .setState(State.RUNNING)
            .build()
    );
  }

  @Override
  public void onProcessExit(final String processId, final int exitCode) {
    // Note that the order here matters: We need to first inform the Driver's event handlers about the process exit
    // and then release the resources. Otherwise, the Driver might be shutdown because of an idle condition before the
    // message about the evaluator exit could have been sent and processed.
    switch (exitCode) {
    case 0:
      this.onCleanExit(processId);
      break;
    default:
      this.onUncleanExit(processId, exitCode);
    }
    this.resourceManager.get().onEvaluatorExit(processId);
  }

  /**
   * Inform REEF of a cleanly exited process.
   *
   * @param processId
   */
  private void onCleanExit(final String processId) {
    this.onResourceStatus(
        ResourceStatusEventImpl.newBuilder()
            .setIdentifier(processId)
            .setState(State.DONE)
            .setExitCode(0)
            .build()
    );
  }

  /**
   * Inform REEF of an unclean process exit.
   *
   * @param processId
   * @param exitCode
   */
  private void onUncleanExit(final String processId, final int exitCode) {
    this.onResourceStatus(
        ResourceStatusEventImpl.newBuilder()
            .setIdentifier(processId)
            .setState(State.FAILED)
            .setExitCode(exitCode)
            .build()
    );
  }

  private void onResourceStatus(final ResourceStatusEvent resourceStatus) {
    LOG.log(Level.INFO, "Sending resource status: {0} ", resourceStatus);

    // Here, we introduce an arbitrary wait. This is to make sure that at the exit of an Evaluator, the last
    // heartbeat from that Evaluator arrives before this message. This makes sure that the local runtime behaves like
    // a resource manager with regard to that timing.
    try {
      Thread.sleep(100);
    } catch (final InterruptedException e) {
      LOG.log(Level.FINEST, "Sleep interrupted. Event will be fired earlier than usual.");
    }
    this.resourceStatusHandler.onNext(resourceStatus);
  }

}
