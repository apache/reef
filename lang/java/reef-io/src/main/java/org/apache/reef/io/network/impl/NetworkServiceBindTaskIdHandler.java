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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * If a network service in evaluator does not have a unique id,
 * the network service bind task's id for their identifier when each
 * of task starts.
 */
public final class NetworkServiceBindTaskIdHandler implements EventHandler<TaskStart> {

  private final InjectionFuture<NetworkService> networkService;
  private final IdentifierFactory idFactory;

  @Inject
  public NetworkServiceBindTaskIdHandler(
      final InjectionFuture<NetworkService> networkService,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory) {
    this.networkService = networkService;
    this.idFactory = idFactory;
  }

  @Override
  public void onNext(TaskStart value) {
    networkService.get().registerId(idFactory.getNewInstance(value.getId()));
  }
}
