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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;

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
  public synchronized void onNext(final DriverRuntimeProtocol.ResourceRequestProto resourceRequestProto) {
    LOG.log(Level.FINEST, "Got ResourceRequestProto in YarnResourceRequestHandler: memory = {0}, cores = {1}.", new Object[] {resourceRequestProto.getMemorySize(), resourceRequestProto.getVirtualCores() });

    final String[] nodes = resourceRequestProto.getNodeNameCount() == 0 ? null :
        resourceRequestProto.getNodeNameList().toArray(new String[resourceRequestProto.getNodeNameCount()]);
    final String[] racks = resourceRequestProto.getRackNameCount() == 0 ? null :
        resourceRequestProto.getRackNameList().toArray(new String[resourceRequestProto.getRackNameCount()]);

    // set the priority for the request
    final Priority pri = getPriority(resourceRequestProto);
    final Resource resource = getResource(resourceRequestProto);
    final boolean relax_locality = !resourceRequestProto.hasRelaxLocality() || resourceRequestProto.getRelaxLocality();

    final AMRMClient.ContainerRequest[] containerRequests =
        new AMRMClient.ContainerRequest[resourceRequestProto.getResourceCount()];

    for (int i = 0; i < resourceRequestProto.getResourceCount(); i++) {
      containerRequests[i] = new AMRMClient.ContainerRequest(resource, nodes, racks, pri, relax_locality);
    }
    this.yarnContainerRequestHandler.onContainerRequest(containerRequests);
  }

  private synchronized Resource getResource(final DriverRuntimeProtocol.ResourceRequestProto resourceRequestProto) {
    final Resource result = Records.newRecord(Resource.class);
    final int memory = getMemory(resourceRequestProto.getMemorySize());
    final int core = resourceRequestProto.getVirtualCores();
    LOG.log(Level.FINEST, "Resource requested: memory = {0}, virtual core count = {1}.",  new Object[]{ memory, core } );
    result.setMemory(memory);
    result.setVirtualCores(core);
    return result;
  }

  private synchronized Priority getPriority(final DriverRuntimeProtocol.ResourceRequestProto resourceRequestProto) {
    final Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(resourceRequestProto.hasPriority() ? resourceRequestProto.getPriority() : 1);
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
