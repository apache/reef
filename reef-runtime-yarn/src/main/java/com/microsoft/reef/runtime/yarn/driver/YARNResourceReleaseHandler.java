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

import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ResourceReleaseHandler for YARN.
 */
final class YARNResourceReleaseHandler implements ResourceReleaseHandler {
  private static final Logger LOG = Logger.getLogger(YARNResourceReleaseHandler.class.getName());
  private final YarnContainerManager yarnContainerManager;

  @Inject
  YARNResourceReleaseHandler(final YarnContainerManager yarnContainerManager) {
    this.yarnContainerManager = yarnContainerManager;
    LOG.log(Level.INFO, "Instantiated 'YARNResourceReleaseHandler'");
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceReleaseProto resourceReleaseProto) {
    final String containerId = resourceReleaseProto.getIdentifier();
    LOG.log(Level.FINEST, "Releasing container {0}", containerId);
    this.yarnContainerManager.release(containerId);
  }
}
