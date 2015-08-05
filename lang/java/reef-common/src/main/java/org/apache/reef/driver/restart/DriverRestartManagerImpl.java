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

import javax.inject.Inject;
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

  private boolean restartCompleted;
  private int numPreviousContainers;
  private int numRecoveredContainers;

  @Inject
  private DriverRestartManagerImpl(final DriverRuntimeRestartManager driverRuntimeRestartManager) {
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
    this.restartCompleted = false;
    this.numPreviousContainers = -1;
    this.numRecoveredContainers = 0;
  }

  @Override
  public boolean isRestart() {
    return driverRuntimeRestartManager.isRestart();
  }

  @Override
  public void onRestart() {
    final EvaluatorRestartInfo evaluatorRestartInfo = driverRuntimeRestartManager.getAliveAndFailedEvaluators();
    setNumPreviousContainers(evaluatorRestartInfo.getAliveEvaluators().size());
    driverRuntimeRestartManager.informAboutEvaluatorFailures(evaluatorRestartInfo.getFailedEvaluators());
  }

  @Override
  public synchronized void setRestartCompleted() {
    if (this.restartCompleted) {
      LOG.log(Level.WARNING, "Calling setRestartCompleted more than once.");
    } else {
      this.restartCompleted = true;
    }
  }

  @Override
  public synchronized int getNumPreviousContainers() {
    return this.numPreviousContainers;
  }

  @Override
  public synchronized void setNumPreviousContainers(final int num) {
    if (this.numPreviousContainers >= 0) {
      throw new IllegalStateException("Attempting to set the number of expected containers left " +
          "from a previous container more than once.");
    } else {
      this.numPreviousContainers = num;
    }
  }

  @Override
  public synchronized int getNumRecoveredContainers() {
    return this.numRecoveredContainers;
  }

  @Override
  public synchronized void oneContainerRecovered() {
    this.numRecoveredContainers += 1;
    if (this.numRecoveredContainers > this.numPreviousContainers) {
      throw new IllegalStateException("Reconnected to" +
          this.numRecoveredContainers + "Evaluators while only expecting " + this.numPreviousContainers);
    }
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
