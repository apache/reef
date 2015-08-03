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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.yarn.driver.parameters.YarnEvaluatorPreserver;
import org.apache.reef.driver.restart.RestartEvaluatorInfo;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of restart manager for YARN. Handles evaluator preservation as well
 * as evaluator recovery on YARN.
 */
@DriverSide
@RuntimeAuthor
@Private
@Unstable
public final class YarnDriverRestartManager implements DriverRestartManager {

  private static final Logger LOG = Logger.getLogger(YarnDriverRestartManager.class.getName());

  private final EvaluatorPreserver evaluatorPreserver;
  private final ApplicationMasterRegistration registration;
  private final DriverStatusManager driverStatusManager;
  private final REEFEventHandlers reefEventHandlers;
  private List<Container> previousContainers;

  @Inject
  private YarnDriverRestartManager(@Parameter(YarnEvaluatorPreserver.class)
                           final EvaluatorPreserver evaluatorPreserver,
                           final REEFEventHandlers reefEventHandlers,
                           final ApplicationMasterRegistration registration,
                           final DriverStatusManager driverStatusManager){
    this.registration = registration;
    this.evaluatorPreserver = evaluatorPreserver;
    this.driverStatusManager = driverStatusManager;
    this.reefEventHandlers = reefEventHandlers;
    this.previousContainers = null;
  }

  @Override
  public boolean isRestart() {
    if (this.previousContainers == null) {
      this.previousContainers = this.registration.getRegistration().getContainersFromPreviousAttempts();

      // If it's still null, create an empty list to indicate that it's not a restart.
      if (this.previousContainers == null) {
        this.previousContainers = new ArrayList<>();
      }
    }

    if (this.previousContainers.isEmpty()) {
      return false;
    }

    return true;
  }

  @Override
  public RestartEvaluatorInfo onRestartRecoverEvaluators() {
    final Set<String> recoveredEvaluators = new HashSet<>();
    final Set<String> failedEvaluators = new HashSet<>();

    if (this.previousContainers != null && !this.previousContainers.isEmpty()) {
      LOG.log(Level.INFO, "Driver restarted, with {0} previous containers", this.previousContainers.size());
      final Set<String> expectedContainers = this.evaluatorPreserver.recoverEvaluators();

      final int numExpectedContainers = expectedContainers.size();
      final int numPreviousContainers = this.previousContainers.size();
      if (numExpectedContainers > numPreviousContainers) {
        // we expected more containers to be alive, some containers must have died during driver restart
        LOG.log(Level.WARNING, "Expected {0} containers while only {1} are still alive",
            new Object[]{numExpectedContainers, numPreviousContainers});
        final Set<String> previousContainersIds = new HashSet<>();
        for (final Container container : this.previousContainers) {
          previousContainersIds.add(container.getId().toString());
        }
        for (final String expectedContainerId : expectedContainers) {
          if (!previousContainersIds.contains(expectedContainerId)) {
            this.evaluatorPreserver.recordRemovedEvaluator(expectedContainerId);
            LOG.log(Level.WARNING, "Expected container [{0}] not alive, must have failed during driver restart.",
                expectedContainerId);
            failedEvaluators.add(expectedContainerId);
          }
        }
      }
      if (numExpectedContainers < numPreviousContainers) {
        // somehow we have more alive evaluators, this should not happen
        throw new RuntimeException("Expected only [" + numExpectedContainers + "] containers " +
            "but resource manager believe that [" + numPreviousContainers + "] are outstanding for driver.");
      }

      //  numExpectedContainers == numPreviousContainers
      for (final Container container : this.previousContainers) {
        LOG.log(Level.FINE, "Previous container: [{0}]", container.toString());
        if (!expectedContainers.contains(container.getId().toString())) {
          throw new RuntimeException("Not expecting container " + container.getId().toString());
        }

        recoveredEvaluators.add(container.getId().toString());
      }
    }

    return new RestartEvaluatorInfo(recoveredEvaluators, failedEvaluators);
  }

  @Override
  public void informAboutEvaluatorAlive(final Set<String> evaluatorIds) {
    // We will wait for these evaluators to contact us, so we do not need to record the entire container information.
    this.driverStatusManager.setNumPreviousContainers(evaluatorIds.size());
  }

  @Override
  public void informAboutEvaluatorFailures(final Set<String> evaluatorIds) {
    for (String evaluatorId : evaluatorIds) {
      LOG.log(Level.WARNING, "Container [" + evaluatorId +
          "] has failed during driver restart process, FailedEvaluatorHandler will be triggered, but " +
          "no additional evaluator can be requested due to YARN-2433.");
      // trigger a failed evaluator event
      this.reefEventHandlers.onResourceStatus(ResourceStatusEventImpl.newBuilder()
          .setIdentifier(evaluatorId)
          .setState(ReefServiceProtos.State.FAILED)
          .setExitCode(1)
          .setDiagnostics("Container [" + evaluatorId + "] failed during driver restart process.")
          .setIsFromPreviousDriver(true)
          .build());
    }
  }

  @Override
  public void recordAllocatedEvaluator(final String id) {
    this.evaluatorPreserver.recordAllocatedEvaluator(id);
  }

  @Override
  public void recordRemovedEvaluator(final String id) {
    this.evaluatorPreserver.recordRemovedEvaluator(id);
  }
}
