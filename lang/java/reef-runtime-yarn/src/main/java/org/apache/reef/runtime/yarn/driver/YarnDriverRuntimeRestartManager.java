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

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.driver.restart.DriverRuntimeRestartManager;
import org.apache.reef.driver.restart.EvaluatorRestartInfo;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.yarn.driver.parameters.YarnEvaluatorPreserver;
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

  private final EvaluatorPreserver evaluatorPreserver;
  private final ApplicationMasterRegistration registration;
  private final REEFEventHandlers reefEventHandlers;
  private final YarnContainerManager yarnContainerManager;

  private Set<Container> previousContainers;

  @Inject
  private YarnDriverRuntimeRestartManager(@Parameter(YarnEvaluatorPreserver.class)
                                          final EvaluatorPreserver evaluatorPreserver,
                                          final REEFEventHandlers reefEventHandlers,
                                          final ApplicationMasterRegistration registration,
                                          final YarnContainerManager yarnContainerManager) {
    this.registration = registration;
    this.evaluatorPreserver = evaluatorPreserver;
    this.reefEventHandlers = reefEventHandlers;
    this.yarnContainerManager = yarnContainerManager;
    this.previousContainers = null;
  }

  /**
   * Determines whether the application master has been restarted based on the container ID environment
   * variable provided by YARN. If that fails, determine whether the application master is a restart
   * based on the number of previous containers reported by YARN.
   * @return true if the application master is a restarted instance, false otherwise.
   */
  @Override
  public boolean hasRestarted() {
    final String containerIdString = getContainerIdString();

    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      LOG.log(Level.WARNING, "Container ID is null, determining restart based on previous containers.");
      return this.isRestartByPreviousContainers();
    }

    final ApplicationAttemptId appAttemptID = getAppAttemptId(containerIdString);

    if (appAttemptID == null) {
      LOG.log(Level.WARNING, "applicationAttempt ID is null, determining restart based on previous containers.");
      return this.isRestartByPreviousContainers();
    }

    LOG.log(Level.FINE, "Application attempt: " + appAttemptID.getAttemptId());

    return appAttemptID.getAttemptId() > 1;
  }

  private static String getContainerIdString() {
    try {
      return System.getenv(ApplicationConstants.Environment.CONTAINER_ID.key());
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Unable to get the container ID from the environment, exception " +
          e + " was thrown.");
      return null;
    }
  }

  private static ApplicationAttemptId getAppAttemptId(final String containerIdString) {
    try {
      final ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
      return containerId.getApplicationAttemptId();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Unable to get the applicationAttempt ID from the environment, exception " +
          e + " was thrown.");
      return null;
    }
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
   * Used by tDriverRestartManager. Gets the list of previous containers from the resource manager,
   * compares that list to the YarnDriverRuntimeRestartManager's own list based on the evalutor preserver,
   * and determine which evaluators are alive and which have failed during restart.
   * @return EvaluatorRestartInfo, the object encapsulating alive and failed evaluator IDs.
   */
  @Override
  public EvaluatorRestartInfo getAliveAndFailedEvaluators() {
    final Set<String> recoveredEvaluators = new HashSet<>();
    final Set<String> failedEvaluators = new HashSet<>();

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

    return new EvaluatorRestartInfo(recoveredEvaluators, failedEvaluators);
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
          .setState(ReefServiceProtos.State.FAILED)
          .setExitCode(1)
          .setDiagnostics("Container [" + evaluatorId + "] failed during driver restart process.")
          .setIsFromPreviousDriver(true)
          .build());
    }
  }
}
