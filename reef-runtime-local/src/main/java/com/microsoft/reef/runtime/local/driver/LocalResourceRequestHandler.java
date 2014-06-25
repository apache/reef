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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;

import javax.inject.Inject;

/**
 * Takes resource requests and patches them through to the ResourceManager
 */
@Private
@DriverSide
final class LocalResourceRequestHandler implements ResourceRequestHandler {

  private final ResourceManager resourceManager;

  @Inject
  LocalResourceRequestHandler(final ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceRequestProto t) {
    this.resourceManager.onResourceRequest(t);
  }
}
