/**
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
package org.apache.reef.runtime.local.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.DriverRuntimeProtocol;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Manages a queue of resource requests.
 */
@Private
@DriverSide
final class ResourceRequestQueue {

  private final BlockingQueue<ResourceRequest> requestQueue = new LinkedBlockingQueue<>();

  /**
   * Add a request to the end of the queue.
   *
   * @param req the request to be added.
   */
  final void add(final ResourceRequest req) {
    this.requestQueue.add(req);
  }

  /**
   * @return true if there are outstanding resource requests. false otherwise.
   */
  final boolean hasOutStandingRequests() {
    return !this.requestQueue.isEmpty();
  }

  /**
   * Satisfies one resource for the front-most request. If that satisfies the
   * request, it is removed from the queue.
   */
  final synchronized DriverRuntimeProtocol.ResourceRequestProto satisfyOne() {
    final ResourceRequest req = this.requestQueue.element();
    req.satisfyOne();
    if (req.isSatisfied()) {
      this.requestQueue.poll();
    }
    return req.getRequestProto();
  }

  final synchronized int getNumberOfOutstandingRequests() {
    return this.requestQueue.size();
  }

}
