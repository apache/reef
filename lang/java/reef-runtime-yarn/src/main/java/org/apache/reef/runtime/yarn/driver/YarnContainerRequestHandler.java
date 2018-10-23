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
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface to request containers from YARN using AMRMClient.ContainerRequest requests.
 */
@DefaultImplementation(YarnContainerRequestHandlerImpl.class)
public interface YarnContainerRequestHandler {
  /**
   * Enqueue a set of container requests with YARN.
   *
   * @param containerRequests set of container requests
   */
  void onContainerRequest(final String requestId, final AMRMClient.ContainerRequest... containerRequests);

  /**
   * Container requests without request id. Will be deprecated in 0.18.
   * @param containerRequests
   */
  @Deprecated
  void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests);

  void onContainerRequestRemove(String requestId);
}
