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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.EvaluatorRequestorImpl;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;
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
  private AtomicLong allocationRequestId = new AtomicLong();

  @Inject
  YarnResourceRequestHandler(final YarnContainerRequestHandler yarnContainerRequestHandler,
                             final ApplicationMasterRegistration registration) {
    this.yarnContainerRequestHandler = yarnContainerRequestHandler;
    this.registration = registration;
  }

  @Override
  public void onNext(final ResourceRequestEvent resourceRequestEvent) {
    LOG.log(Level.FINEST, "YarnResourceRequestHandler.onNext request for id {0} with resource count {1}.",
        new Object[] {resourceRequestEvent.getRequestId(), resourceRequestEvent.getResourceCount()});

    if (resourceRequestEvent.getResourceCount() == EvaluatorRequestorImpl.REMOVE_FLAG) {
      removeRequest(resourceRequestEvent.getRequestId());
      return;
    }
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "Got ResourceRequestEvent in YarnResourceRequestHandler: memory = {0}, cores = {1}," +
              "nodes: {2}, racks: {3}, resourceCount: {4}, requestId: {5}.",
          new Object[]{resourceRequestEvent.getMemorySize(), resourceRequestEvent.getVirtualCores(),
              resourceRequestEvent.getNodeNameList().size(), resourceRequestEvent.getRackNameList().size(),
              resourceRequestEvent.getResourceCount(), resourceRequestEvent.getRequestId()});
    }

    final String[] nodes = resourceRequestEvent.getNodeNameList().size() == 0 ? null :
        resourceRequestEvent.getNodeNameList().toArray(new String[resourceRequestEvent.getNodeNameList().size()]);
    final String[] racks = resourceRequestEvent.getRackNameList().size() == 0 ? null :
        resourceRequestEvent.getRackNameList().toArray(new String[resourceRequestEvent.getRackNameList().size()]);

    // set the priority for the request
    final Priority pri = getPriority(resourceRequestEvent);
    final Resource resource = getResource(resourceRequestEvent);
    final boolean relaxLocality = resourceRequestEvent.getRelaxLocality().orElse(true);
    final String nodeLabelExpression = resourceRequestEvent.getNodeLabelExpression().orElse("");

    final int count = resourceRequestEvent.getResourceCount();
    final AMRMClient.ContainerRequest[] containerRequests = new AMRMClient.ContainerRequest[count];

    final boolean noNodes = nodes == null;
    if (noNodes && count > 1) {
      LOG.log(Level.WARNING, "Number of containers requested is {0} but node names is not null.", count);
    }

    for (int i = 0; i < count; i++) {
      long nextId = allocationRequestId.incrementAndGet();
      containerRequests[i] =
          new AMRMClient.ContainerRequest(resource, nodes, racks, pri, nextId, relaxLocality,
              StringUtils.isEmpty(nodeLabelExpression) ? null : nodeLabelExpression);
      LOG.log(Level.FINE, "Creating ContainerRequest for allocationRequest id: {0}.", nextId);
    }

    this.yarnContainerRequestHandler.onContainerRequest(resourceRequestEvent.getRequestId(), containerRequests);
  }

  private void removeRequest(final String requestId) {
    LOG.log(Level.INFO, "YarnResourceRequestHandler.removeRequest for requestId: {0}", requestId);
    this.yarnContainerRequestHandler.onContainerRequestRemove(requestId);
  }

  private Resource getResource(final ResourceRequestEvent resourceRequestEvent) {
    final Resource result = Records.newRecord(Resource.class);
    final int memory = getMemory(resourceRequestEvent.getMemorySize().get());
    final int core = resourceRequestEvent.getVirtualCores().get();
    LOG.log(Level.FINE, "Resource requested: memory = {0}, virtual core count = {1}.", new Object[]{memory, core});
    result.setMemory(memory);
    result.setVirtualCores(core);
    return result;
  }

  private static Priority getPriority(final ResourceRequestEvent resourceRequestEvent) {
    final Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(resourceRequestEvent.getPriority().orElse(1));
    return pri;
  }

  private int getMemory(final int requestedMemory) {
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
