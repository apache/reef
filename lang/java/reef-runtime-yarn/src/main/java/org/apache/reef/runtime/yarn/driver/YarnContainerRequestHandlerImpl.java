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

import org.apache.hadoop.yarn.client.api.AMRMClient;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Side-channel to request containers from YARN using AMRMClient.ContainerRequest requests.
 */
public final class YarnContainerRequestHandlerImpl implements YarnContainerRequestHandler {
  private static final Logger LOG = Logger.getLogger(YarnContainerRequestHandlerImpl.class.getName());

  private final YarnContainerManager containerManager;


  @Inject
  YarnContainerRequestHandlerImpl(final YarnContainerManager containerManager) {
    this.containerManager = containerManager;
    LOG.log(Level.FINEST, "Instantiated 'YarnContainerRequestHandler'");
  }

  /**
   * Container requests without request id. Will be deprecated in 0.18.
   * @param containerRequests
   */
  @Deprecated
  @Override
  public void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests) {
    LOG.log(Level.FINEST, "Sending container requests to YarnContainerManager.");
    this.containerManager.onContainerRequest(containerRequests);
  }

  @Override
  public void onContainerRequest(final String requestId, final AMRMClient.ContainerRequest... containerRequests) {
    LOG.log(Level.FINEST, "Sending container requests to YarnContainerManager.");
    this.containerManager.onContainerRequest(requestId, containerRequests);
  }

  @Override
  public void onContainerRequestRemove(final String requestId) {
    LOG.log(Level.FINEST, "Sending container request remove to YarnContainerManager.");
    this.containerManager.onContainerRequestRemove(requestId);
  }
}
