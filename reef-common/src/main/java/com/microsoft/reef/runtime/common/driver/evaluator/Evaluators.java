/**
 * Copyright (C) 201 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.util.Optional;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages all Evaluators. See EvaluatorManager for the Driver side representation of a single Evaluator.
 */
@DriverSide
@Private
public final class Evaluators {
  private static final Logger LOG = Logger.getLogger(Evaluators.class.getName());
  private final Map<String, EvaluatorManager> evaluators = new HashMap<>();
  private final ResourceCatalog resourceCatalog;
  private final EvaluatorManagerFactory evaluatorManagerFactory;

  @Inject
  Evaluators(final ResourceCatalog resourceCatalog, final EvaluatorManagerFactory evaluatorManagerFactory) {
    this.resourceCatalog = resourceCatalog;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
  }

  /**
   * @param evaluatorID
   * @return true, if an EvaluatorManager with the given ID is known.
   */
  private synchronized boolean isKnown(final String evaluatorID) {
    return this.evaluators.containsKey(evaluatorID);
  }

  /**
   * @param evaluatorManager the EvaluatorManager to be removed.
   * @throws java.lang.RuntimeException if no such EvaluatorManager exists.
   */
  synchronized void remove(final EvaluatorManager evaluatorManager) {
    if (this.isKnown(evaluatorManager.getId())) {
      this.evaluators.remove(evaluatorManager.getId());
    } else {
      throw new RuntimeException("Trying t remove an unknown EvaluatorManager: " + evaluatorManager.getId());
    }
  }

  /**
   * Return true if <em>all</em> evaluators are in closed state
   * (and their processing queues are empty).
   */
  public synchronized boolean allEvaluatorsAreClosed() {
    synchronized (this.evaluators) {
      for (final EvaluatorManager eval : this.evaluators.values()) {
        if (!eval.isClosed()) {
          return false;
        }
      }
    }
    return true;
  }

  public synchronized Optional<EvaluatorManager> get(final String evaluatorId) {
    if (this.isKnown(evaluatorId)) {
      return Optional.of(this.evaluators.get(evaluatorId));
    } else {
      return Optional.empty();
    }
  }

  private synchronized void put(final EvaluatorManager evaluatorManager) {
    final String evaluatorId = evaluatorManager.getId();
    if (this.isKnown(evaluatorId)) {
      throw new IllegalArgumentException("Trying to re-add an Evaluator that is already known: " + evaluatorId);
    } else {
      this.evaluators.put(evaluatorId, evaluatorManager);
    }
  }

  public synchronized void close() {
    for (final EvaluatorManager evaluatorManager : new ArrayList<>(this.evaluators.values())) {
      LOG.log(Level.WARNING, "Unclean shutdown of evaluator {0}", evaluatorManager.getId());
      evaluatorManager.close();
    }
  }

  public synchronized void addEvaluatorManagerFromAllocation(final DriverRuntimeProtocol.ResourceAllocationProto resourceAllocationProto) {
    this.put(this.evaluatorManagerFactory.getNewEvaluatorManager(resourceAllocationProto));
  }


}
