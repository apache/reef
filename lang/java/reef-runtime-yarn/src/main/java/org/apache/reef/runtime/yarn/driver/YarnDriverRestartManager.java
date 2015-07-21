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
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.yarn.driver.parameters.YarnEvaluatorPreserver;
import org.apache.reef.runtime.yarn.util.YarnTypes;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class YarnDriverRestartManager implements DriverRestartManager{

  private static final Logger LOG = Logger.getLogger(YarnDriverRestartManager.class.getName());

  private final EvaluatorPreserver evaluatorPreserver;
  private final ApplicationMasterRegistration registration;
  private final DriverStatusManager driverStatusManager;
  private final REEFEventHandlers reefEventHandlers;

  @Inject
  YarnDriverRestartManager(@Parameter(YarnEvaluatorPreserver.class)
                           final EvaluatorPreserver evaluatorPreserver,
                           final REEFEventHandlers reefEventHandlers,
                           final ApplicationMasterRegistration registration,
                           final DriverStatusManager driverStatusManager){
    this.registration = registration;
    this.evaluatorPreserver = evaluatorPreserver;
    this.driverStatusManager = driverStatusManager;
    this.reefEventHandlers = reefEventHandlers;
  }

  @Override
  public boolean isRestart() {
    Map<String, String> envs = System.getenv();
    String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.key());
    if (containerIdString == null) {
      // container id should always be set in the env by the framework
      LOG.log(Level.WARNING, "Unable to get the container ID from the environment. Treating as a non-restart.");
      return false;
    }
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
    if (appAttemptID == null) {
      LOG.log(Level.WARNING, "Unable to get the applicationAttempt ID from the environment. " +
          "Treating as a non-restart.");
      return false;
    }

    LOG.log(Level.FINE, "Application attempt: " + appAttemptID.getAttemptId());

    return appAttemptID.getAttemptId() > 1;
  }

  @Override
  public void onRestartRecoverEvaluators() {
    // TODO: this is currently being developed on a hacked 2.4.0 bits, should be 2.4.1
    final String minVersionToGetPreviousContainer = "2.4.0";

    // when supported, obtain the list of the containers previously allocated, and write info to driver folder
    if (YarnTypes.isAtOrAfterVersion(minVersionToGetPreviousContainer)) {
      LOG.log(Level.FINEST, "Hadoop version is {0} or after with support to retain previous containers, " +
          "processing previous containers.", minVersionToGetPreviousContainer);
      processPreviousContainers();
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

  private void processPreviousContainers() {
    final List<Container> previousContainers = this.registration.getRegistration().getContainersFromPreviousAttempts();
    if (previousContainers != null && !previousContainers.isEmpty()) {
      LOG.log(Level.INFO, "Driver restarted, with {0} previous containers", previousContainers.size());
      this.driverStatusManager.setNumPreviousContainers(previousContainers.size());

      final Set<String> expectedContainers = this.evaluatorPreserver.recoverEvaluators();

      final int numExpectedContainers = expectedContainers.size();
      final int numPreviousContainers = previousContainers.size();
      if (numExpectedContainers > numPreviousContainers) {
        // we expected more containers to be alive, some containers must have died during driver restart
        LOG.log(Level.WARNING, "Expected {0} containers while only {1} are still alive",
            new Object[]{numExpectedContainers, numPreviousContainers});
        final Set<String> previousContainersIds = new HashSet<>();
        for (final Container container : previousContainers) {
          previousContainersIds.add(container.getId().toString());
        }
        for (final String expectedContainerId : expectedContainers) {
          if (!previousContainersIds.contains(expectedContainerId)) {
            this.evaluatorPreserver.recordRemovedEvaluator(expectedContainerId);
            LOG.log(Level.WARNING, "Expected container [{0}] not alive, must have failed during driver restart.",
                expectedContainerId);
            informAboutContainerFailureDuringRestart(expectedContainerId);
          }
        }
      }
      if (numExpectedContainers < numPreviousContainers) {
        // somehow we have more alive evaluators, this should not happen
        throw new RuntimeException("Expected only [" + numExpectedContainers + "] containers " +
            "but resource manager believe that [" + numPreviousContainers + "] are outstanding for driver.");
      }

      //  numExpectedContainers == numPreviousContainers
      for (final Container container : previousContainers) {
        LOG.log(Level.FINE, "Previous container: [{0}]", container.toString());
        if (!expectedContainers.contains(container.getId().toString())) {
          throw new RuntimeException("Not expecting container " + container.getId().toString());
        }

        handleRestartedContainer(container);
      }
    }
  }

  private void informAboutContainerFailureDuringRestart(final String containerId) {
    LOG.log(Level.WARNING, "Container [" + containerId +
        "] has failed during driver restart process, FailedEvaluatorHandler will be triggered, but " +
        "no additional evaluator can be requested due to YARN-2433.");
    // trigger a failed evaluator event
    this.reefEventHandlers.onResourceStatus(ResourceStatusEventImpl.newBuilder()
        .setIdentifier(containerId)
        .setState(ReefServiceProtos.State.FAILED)
        .setExitCode(1)
        .setDiagnostics("Container [" + containerId + "] failed during driver restart process.")
        .setIsFromPreviousDriver(true)
        .build());
  }

  private void handleRestartedContainer(Container container) {
    // TODO: implement logic
  }
}
