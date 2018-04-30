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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.driver.restart.DriverRuntimeRestartManager;
import org.apache.reef.driver.restart.EvaluatorRestartInfo;
import org.apache.reef.driver.restart.RestartEvaluators;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.yarn.driver.parameters.YarnEvaluatorPreserver;
import org.apache.reef.runtime.yarn.util.YarnUtilities;
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
public final class YarnDriverRuntimeRestartManager implements DriverRuntimeRestartManager {

  private static final Logger LOG = Logger.getLogger(YarnDriverRuntimeRestartManager.class.getName());

  /**
   * The default resubmission attempts number returned if:
   * 1) we are not able to determine the number of application attempts based on the environment provided by YARN.
   * 2) we are able to receive a list of previous containers from the Resource Manager.
   */
  private static final int DEFAULT_RESTART_RESUBMISSION_ATTEMPTS = 1;

  private final EvaluatorPreserver evaluatorPreserver;
  private final ApplicationMasterRegistration registration;
  private final REEFEventHandlers reefEventHandlers;
  private final YarnContainerManager yarnContainerManager;
  private final RackNameFormatter rackNameFormatter;

  private Set<Container> previousContainers = null;

  @Inject
  private YarnDriverRuntimeRestartManager(@Parameter(YarnEvaluatorPreserver.class)
                                          final EvaluatorPreserver evaluatorPreserver,
                                          final REEFEventHandlers reefEventHandlers,
                                          final ApplicationMasterRegistration registration,
                                          final YarnContainerManager yarnContainerManager,
                                          final RackNameFormatter rackNameFormatter) {
    this.registration = registration;
    this.evaluatorPreserver = evaluatorPreserver;
    this.reefEventHandlers = reefEventHandlers;
    this.yarnContainerManager = yarnContainerManager;
    this.rackNameFormatter = rackNameFormatter;
  }

  /**
   * Determines the number of times the Driver has been submitted based on the container ID environment
   * variable provided by YARN. If that fails, determine whether the application master is a restart
   * based on the number of previous containers reported by YARN. In the failure scenario, returns 1 if restart, 0
   * otherwise.
   * @return positive value if the application master is a restarted instance, 0 otherwise.
   */
  @Override
  public int getResubmissionAttempts() {
    final String containerIdString = YarnUtilities.getContainerIdString();
    final ApplicationAttemptId appAttemptID = YarnUtilities.getAppAttemptId(containerIdString);

    if (containerIdString == null || appAttemptID == null) {
      LOG.log(Level.WARNING, "Was not able to fetch application attempt, container ID is [" + containerIdString +
          "] and application attempt is [" + appAttemptID + "]. Determining restart based on previous containers.");

      if (this.isRestartByPreviousContainers()) {
        LOG.log(Level.WARNING, "Driver is a restarted instance based on the number of previous containers. " +
            "As returned by the Resource Manager. Returning default resubmission attempts " +
            DEFAULT_RESTART_RESUBMISSION_ATTEMPTS + ".");
        return DEFAULT_RESTART_RESUBMISSION_ATTEMPTS;
      }

      return 0;
    }

    int appAttempt = appAttemptID.getAttemptId();

    LOG.log(Level.FINE, "Application attempt: " + appAttempt);
    assert appAttempt > 0;
    return appAttempt - 1;
  }

  /**
   * Initializes the list of previous containers and determine whether or not this is an instance of restart
   * based on information reported by the RM.
   * @return true if previous containers is not empty.
   */
  private boolean isRestartByPreviousContainers() {
    this.initializeListOfPreviousContainers();
    return !this.previousContainers.isEmpty();
  }

  /**
   * Initializes the list of previous containers as reported by YARN.
   */
  private synchronized void initializeListOfPreviousContainers() {
    if (this.previousContainers == null) {
      final List<Container> yarnPrevContainers =
          this.registration.getRegistration().getContainersFromPreviousAttempts();

      // If it's still null, create an empty list to indicate that it's not a restart.
      if (yarnPrevContainers == null) {
        this.previousContainers = Collections.unmodifiableSet(new HashSet<Container>());
      } else {
        this.previousContainers = Collections.unmodifiableSet(new HashSet<>(yarnPrevContainers));
      }

      yarnContainerManager.onContainersRecovered(this.previousContainers);
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

  /**
   * Used by {@link org.apache.reef.driver.restart.DriverRestartManager}.
   * Gets the list of previous containers from the resource manager,
   * compares that list to the YarnDriverRuntimeRestartManager's own list based on the evaluator preserver,
   * and determine which evaluators are alive and which have failed during restart.
   * @return a map of Evaluator ID to {@link EvaluatorRestartInfo} for evaluators that have either failed or survived
   * driver restart.
   */
  @Override
  public RestartEvaluators getPreviousEvaluators() {
    final RestartEvaluators.Builder restartEvaluatorsBuilder = RestartEvaluators.newBuilder();

    this.initializeListOfPreviousContainers();

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
            LOG.log(Level.WARNING, "Expected container [{0}] not alive, must have failed during driver restart.",
                expectedContainerId);
            restartEvaluatorsBuilder.addRestartEvaluator(
                EvaluatorRestartInfo.createFailedEvaluatorInfo(expectedContainerId));
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

        restartEvaluatorsBuilder.addRestartEvaluator(EvaluatorRestartInfo.createExpectedEvaluatorInfo(
            ResourceEventImpl.newRecoveryBuilder().setIdentifier(container.getId().toString())
                .setNodeId(container.getNodeId().toString()).setRackName(rackNameFormatter.getRackName(container))
                .setResourceMemory(container.getResource().getMemory())
                .setVirtualCores(container.getResource().getVirtualCores())
                .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME).build()));
      }
    }

    return restartEvaluatorsBuilder.build();
  }

  /**
   * Calls the appropriate handler via REEFEventHandlers, which is a runtime specific implementation
   * of the YARN runtime.
   * @param evaluatorIds the set of evaluator IDs of failed evaluators during restart.
   */
  @Override
  public void informAboutEvaluatorFailures(final Set<String> evaluatorIds) {
    for (String evaluatorId : evaluatorIds) {
      LOG.log(Level.WARNING, "Container [" + evaluatorId +
          "] has failed during driver restart process, FailedEvaluatorHandler will be triggered, but " +
          "no additional evaluator can be requested due to YARN-2433.");
      // trigger a failed evaluator event
      this.reefEventHandlers.onResourceStatus(ResourceStatusEventImpl.newBuilder()
          .setIdentifier(evaluatorId)
          .setState(State.FAILED)
          .setExitCode(1)
          .setDiagnostics("Container [" + evaluatorId + "] failed during driver restart process.")
          .build());
    }
  }
}
