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
package org.apache.reef.examples.network;

import org.apache.reef.io.network.NetworkEvent;
import org.apache.reef.io.network.NetworkEventHandler;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Gets odd integer event and sends even integer event.
 */
public final class OddIntegerEventHandler implements NetworkEventHandler<IntegerEvent> {

  private static final Logger LOG = Logger.getLogger(OddIntegerEventHandler.class.getName());

  private final Monitor monitor;
  private final InjectionFuture<NetworkService> networkService;

  @Inject
  public OddIntegerEventHandler(
      final Monitor monitor,
      final InjectionFuture<NetworkService> networkService) {
    this.networkService = networkService;
    this.monitor = monitor;
  }

  @Override
  public void onNext(NetworkEvent<IntegerEvent> value) {
    LOG.log(Level.INFO, value.toString());
    if (value.getEventAt(0).getInt() == 999) {
      monitor.monitorNotify();
    }

    networkService.get().sendEvent(value.getRemoteId(), new IntegerEvent(value.getEventAt(0).getInt() + 1));
  }
}
