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

package org.apache.reef.io.network.group.impl;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.group.api.task.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.config.parameters.GroupCommServiceId;
import org.apache.reef.io.network.group.impl.driver.GroupCommLinkListener;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

public class GroupCommNetworkServiceStartHandler implements EventHandler<ContextStart> {

  private final NetworkService networkService;
  private final GroupCommunicationMessageCodec nsGCMcodec;
  private final GroupCommNetworkHandler nsGCMhandler;
  private final GroupCommLinkListener nsGCMlistener;

  @Inject
  public GroupCommNetworkServiceStartHandler(
      final NetworkService networkService,
      final GroupCommunicationMessageCodec nsGCMcodec,
      final GroupCommNetworkHandler nsGCMhandler,
      final GroupCommLinkListener nsGCMlistener) {
    this.networkService = networkService;
    this.nsGCMcodec = nsGCMcodec;
    this.nsGCMhandler = nsGCMhandler;
    this.nsGCMlistener = nsGCMlistener;
  }

  @Override
  public void onNext(ContextStart value) {
    try {
      networkService.registerConnectionFactory(GroupCommServiceId.class, nsGCMcodec, nsGCMhandler, nsGCMlistener);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
