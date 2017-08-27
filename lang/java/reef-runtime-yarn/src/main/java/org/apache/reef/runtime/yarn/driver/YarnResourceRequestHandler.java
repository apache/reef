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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.yarn.util.YarnUtilities;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Accepts resource requests from the REEF layer, translates them into requests for YARN and hands them to the
 * appropriate handler for those.
 */
@DriverSide
@Private
public final class YarnResourceRequestHandler implements ResourceRequestHandler {

  private static final Logger LOG = Logger.getLogger(YarnResourceRequestHandler.class.getName());
  private final YarnContainerRequestHandler yarnContainerRequestHandler;
  private final ApplicationMasterRegistration registration;

  @Inject
  YarnResourceRequestHandler(final YarnContainerRequestHandler yarnContainerRequestHandler,
                             final ApplicationMasterRegistration registration) {
    this.yarnContainerRequestHandler = yarnContainerRequestHandler;
    this.registration = registration;
  }

  @Override
  public synchronized void onNext(final ResourceRequestEvent resourceRequestEvent) {
    LOG.log(Level.FINEST, "Got ResourceRequestEvent in YarnResourceRequestHandler: memory = {0}, cores = {1}.",
        new Object[]{resourceRequestEvent.getMemorySize(), resourceRequestEvent.getVirtualCores()});

    final String[] nodes = resourceRequestEvent.getNodeNameList().size() == 0 ? null :
        resourceRequestEvent.getNodeNameList().toArray(new String[resourceRequestEvent.getNodeNameList().size()]);
    final String[] racks = resourceRequestEvent.getRackNameList().size() == 0 ? null :
        resourceRequestEvent.getRackNameList().toArray(new String[resourceRequestEvent.getRackNameList().size()]);

    // set the priority for the request
    final Priority pri = getPriority(resourceRequestEvent);
    final Resource resource = getResource(resourceRequestEvent);
    final boolean relaxLocality = resourceRequestEvent.getRelaxLocality().orElse(true);
    final String nodeLabelExpression = resourceRequestEvent.getNodeLabels().get(YarnUtilities.REEF_YARN_NODE_LABEL_ID);

    final AMRMClient.ContainerRequest[] containerRequests =
        new AMRMClient.ContainerRequest[resourceRequestEvent.getResourceCount()];


    for (int i = 0; i < resourceRequestEvent.getResourceCount(); i++) {
      if (nodeLabelExpression == null) {
        containerRequests[i] = new AMRMClient.ContainerRequest(resource, nodes, racks, pri, relaxLocality);
      } else {
        containerRequests[i] = new AMRMClient.ContainerRequest(resource, nodes, racks, pri, relaxLocality,
            nodeLabelExpression);
      }
    }

    this.yarnContainerRequestHandler.onContainerRequest(containerRequests);
  }

  private synchronized Resource getResource(final ResourceRequestEvent resourceRequestEvent) {
    final Resource result = Records.newRecord(Resource.class);
    final int memory = getMemory(resourceRequestEvent.getMemorySize().get());
    final int core = resourceRequestEvent.getVirtualCores().get();
    LOG.log(Level.FINEST, "Resource requested: memory = {0}, virtual core count = {1}.", new Object[]{memory, core});
    result.setMemory(memory);
    result.setVirtualCores(core);
    return result;
  }

  private synchronized Priority getPriority(final ResourceRequestEvent resourceRequestEvent) {
    final Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(resourceRequestEvent.getPriority().orElse(1));
    return pri;
  }

  private synchronized int getMemory(final int requestedMemory) {
    final int result;
    if (!this.registration.isPresent()) {
      LOG.log(Level.WARNING, "AM doesn't seem to be registered. Proceed with fingers crossed.");
      result = requestedMemory;
    } else {
      final int maxMemory = registration.getRegistration().getMaximumResourceCapability().getMemory();
      if (requestedMemory > maxMemory) {
        LOG.log(Level.WARNING, "Asking for {0}MB of memory, but max on this cluster is {1}MB ",
            new Object[]{requestedMemory, maxMemory});
        result = maxMemory;
      } else {
        result = requestedMemory;
      }
    }
    return result;
  }


}
