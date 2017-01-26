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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManagerFactory;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
 * about the current state of a given resource. Ideally, we should think the same thing.
 */
@Private
public final class ResourceStatusHandler implements EventHandler<ResourceStatusEvent> {
  private static final Logger LOG = Logger.getLogger(Evaluators.class.getName());

  private final Evaluators evaluators;
  private final EvaluatorManagerFactory evaluatorManagerFactory;
  private final InjectionFuture<DriverRestartManager> driverRestartManager;

  @Inject
  ResourceStatusHandler(final Evaluators evaluators,
                        final EvaluatorManagerFactory evaluatorManagerFactory,
                        final InjectionFuture<DriverRestartManager> driverRestartManager) {
    this.evaluators = evaluators;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
    this.driverRestartManager = driverRestartManager;
  }

  /**
   * This resource status message comes from the ResourceManager layer, telling me what it thinks
   * about the state of the resource executing an Evaluator. This method simply passes the message
   * off to the referenced EvaluatorManager
   *
   * @param resourceStatusEvent resource status message from the ResourceManager
   */
  @Override
  public void onNext(final ResourceStatusEvent resourceStatusEvent) {

    final String id = resourceStatusEvent.getIdentifier();
    final Optional<EvaluatorManager> evaluatorManager = this.evaluators.get(id);

    LOG.log(Level.FINEST, "Evaluator {0} status: {1}",
        new Object[] {evaluatorManager, resourceStatusEvent.getState()});

    if (evaluatorManager.isPresent()) {
      final EvaluatorManager evaluatorManagerImpl = evaluatorManager.get();
      evaluatorManagerImpl.onResourceStatusMessage(resourceStatusEvent);
      if (evaluatorManagerImpl.isClosed()) {
        this.evaluators.removeClosedEvaluator(evaluatorManagerImpl);
      }

    } else {

      if (this.evaluators.wasClosed(id)) {
        LOG.log(Level.WARNING,
            "Unexpected resource status from closed evaluator {0} with state {1}",
            new Object[] {id, resourceStatusEvent.getState()});
      }

      if (driverRestartManager.get().getEvaluatorRestartState(id).isFailedOrExpired()) {

        final EvaluatorManager previousEvaluatorManager = this.evaluatorManagerFactory
            .getNewEvaluatorManagerForEvaluatorFailedDuringDriverRestart(resourceStatusEvent);

        previousEvaluatorManager.onResourceStatusMessage(resourceStatusEvent);

      } else {
        throw new RuntimeException(
            "Unknown resource status from evaluator " + id + " with state " + resourceStatusEvent.getState());
      }
    }
  }
}
