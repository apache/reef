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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
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
public final class Evaluators implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Evaluators.class.getName());
  /**
   * A map between evaluatorId and the EvaluatorManager that handles this evaluator.
   */
  private final Map<String, EvaluatorManager> evaluators = new HashMap<>();
  /**
   * Resources we know about in the cluster.
   */
  private final ResourceCatalog resourceCatalog;


  @Inject
  Evaluators(final ResourceCatalog resourceCatalog) {
    this.resourceCatalog = resourceCatalog;
  }

  /**
   * Closes all EvaluatorManager instances managed.
   */
  @Override
  public synchronized void close() {
    for (final EvaluatorManager evaluatorManager : new ArrayList<>(this.evaluators.values())) {
      LOG.log(Level.WARNING, "Unclean shutdown of evaluator {0}", evaluatorManager.getId());
      evaluatorManager.close();
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

  /**
   * @param evaluatorId
   * @return the EvaluatorManager for the given id, if one exists.
   */
  public synchronized Optional<EvaluatorManager> get(final String evaluatorId) {
    if (this.evaluators.containsKey(evaluatorId)) {
      return Optional.of(this.evaluators.get(evaluatorId));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Adds an EvaluatorManager.
   *
   * @param evaluatorManager
   * @throws java.lang.IllegalArgumentException if the EvaluatorManager is already known.
   */
  public synchronized void put(final EvaluatorManager evaluatorManager) {
    final String evaluatorId = evaluatorManager.getId();
    if (this.evaluators.containsKey(evaluatorId)) {
      throw new IllegalArgumentException("Trying to re-add an Evaluator that is already known: " + evaluatorId);
    } else {
      this.evaluators.put(evaluatorId, evaluatorManager);
    }
  }

  /**
   * @param evaluatorManager the EvaluatorManager to be removed.
   * @throws java.lang.RuntimeException if no such EvaluatorManager exists.
   */
  public synchronized void remove(final EvaluatorManager evaluatorManager) {
    if (this.evaluators.containsKey(evaluatorManager.getId())) {
      this.evaluators.remove(evaluatorManager.getId());
    } else {
      throw new RuntimeException("Trying t remove an unknown EvaluatorManager: " + evaluatorManager.getId());
    }
  }


}
