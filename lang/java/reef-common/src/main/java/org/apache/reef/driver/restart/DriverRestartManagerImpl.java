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
package org.apache.reef.driver.restart;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.exception.DriverFatalRuntimeException;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of DriverRestartManager. A few methods here are proxy methods for
 * the DriverRuntimeRestartManager that depends on the runtime implementation.
 */
@DriverSide
@Private
@Unstable
public final class DriverRestartManagerImpl implements DriverRestartManager {
  private static final Logger LOG = Logger.getLogger(DriverRestartManagerImpl.class.getName());
  private final DriverRuntimeRestartManager driverRuntimeRestartManager;
  private final Set<String> previousEvaluators;
  private final Set<String> recoveredEvaluators;

  private boolean restartBegan;
  private boolean restartCompleted;

  @Inject
  private DriverRestartManagerImpl(final DriverRuntimeRestartManager driverRuntimeRestartManager) {
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
    this.restartCompleted = false;
    this.restartBegan = false;
    this.previousEvaluators = new HashSet<>();
    this.recoveredEvaluators = new HashSet<>();
  }

  @Override
  public boolean isRestart() {
    return driverRuntimeRestartManager.isRestart();
  }

  @Override
  public void onRestart() {
    final EvaluatorRestartInfo evaluatorRestartInfo = driverRuntimeRestartManager.getAliveAndFailedEvaluators();
    setPreviousEvaluatorIds(evaluatorRestartInfo.getAliveEvaluators());
    driverRuntimeRestartManager.informAboutEvaluatorFailures(evaluatorRestartInfo.getFailedEvaluators());
  }

  @Override
  public boolean isRestartCompleted() {
    return this.restartCompleted;
  }

  @Override
  public synchronized Set<String> getPreviousEvaluatorIds() {
    return Collections.unmodifiableSet(this.previousEvaluators);
  }

  @Override
  public synchronized void setPreviousEvaluatorIds(final Set<String> ids) {
    if (!this.restartBegan) {
      previousEvaluators.addAll(ids);
    } else {
      final String errMsg = "Should not be setting the set of expected alive evaluators more than once.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }
  }

  @Override
  public synchronized Set<String> getRecoveredEvaluatorIds() {
    return Collections.unmodifiableSet(this.previousEvaluators);
  }

  @Override
  public synchronized boolean evaluatorRecovered(final String evaluatorId) {
    if (!this.previousEvaluators.contains(evaluatorId)) {
      final String errMsg = "Evaluator with evaluator ID " + evaluatorId + " not expected to be alive.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    if (!this.recoveredEvaluators.add(evaluatorId)) {
      LOG.log(Level.WARNING, "Evaluator with evaluator ID " + evaluatorId + " added to the set" +
          " of recovered evaluators more than once. Ignoring second add...");
    }

    if (this.recoveredEvaluators.containsAll(this.previousEvaluators)) {
      this.restartCompleted = true;
    }

    return this.restartCompleted;
  }

  @Override
  public void recordAllocatedEvaluator(final String id) {
    driverRuntimeRestartManager.recordAllocatedEvaluator(id);
  }

  @Override
  public void recordRemovedEvaluator(final String id) {
    driverRuntimeRestartManager.recordRemovedEvaluator(id);
  }
}
