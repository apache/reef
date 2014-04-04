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
package com.microsoft.reef.runtime.common.driver.resourcemanager;

import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;

/**
 * Informs the client and then shuts down the driver forcefully in case of Resource Manager errors.
 */
public final class ResourceManagerErrorHandler implements EventHandler<ReefServiceProtos.RuntimeErrorProto> {

  /**
   * A handle to the clock in the Driver. We will use this handle to .stop() the Clock.
   */
  private final Clock clock;
  /**
   * The RemoteManager used to communicate the error to the Client.
   */
  private final RemoteManager remoteManager;
  /**
   * The Client's remote ID.
   */
  private final String clientRemoteID;

  @Inject
  ResourceManagerErrorHandler(final Clock clock,
                              final RemoteManager remoteManager,
                              final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRemoteID) {
    this.clock = clock;
    this.remoteManager = remoteManager;
    this.clientRemoteID = clientRemoteID;
  }

  @Override
  public synchronized void onNext(final ReefServiceProtos.RuntimeErrorProto runtimeErrorProto) {
    final EventHandler<ReefServiceProtos.RuntimeErrorProto> remoteHandler =
        this.remoteManager.getHandler(this.clientRemoteID, ReefServiceProtos.RuntimeErrorProto.class);
    remoteHandler.onNext(runtimeErrorProto);
    this.clock.close();
  }
}
