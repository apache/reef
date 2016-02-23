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
package org.apache.reef.runtime.multi.driver;

import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.multi.driver.parameters.RuntimeName;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;

/**
 * Implementation of a runtime.
 */
final class RuntimeImpl implements Runtime {
  private final String name;
  private final ResourceLaunchHandler resourceLaunchHandler;
  private final ResourceManagerStartHandler resourceManagerStartHandler;
  private final ResourceManagerStopHandler resourceManagerStopHandler;
  private final ResourceReleaseHandler resourceReleaseHandler;
  private final ResourceRequestHandler resourceRequestHandler;

  @Inject
  private RuntimeImpl(
          @Parameter(RuntimeName.class) final String name,
          final ResourceLaunchHandler resourceLaunchHandler,
          final ResourceManagerStartHandler resourceManagerStartHandler,
          final ResourceManagerStopHandler resourceManagerStopHandler,
          final ResourceReleaseHandler resourceReleaseHandler,
          final ResourceRequestHandler resourceRequestHandler) {
    this.name = name;
    this.resourceLaunchHandler = resourceLaunchHandler;
    this.resourceManagerStartHandler = resourceManagerStartHandler;
    this.resourceManagerStopHandler = resourceManagerStopHandler;
    this.resourceReleaseHandler = resourceReleaseHandler;
    this.resourceRequestHandler = resourceRequestHandler;
  }

  @Override
  public String getRuntimeName() {
    return this.name;
  }

  @Override
  public void onResourceLaunch(final ResourceLaunchEvent value) {
    this.resourceLaunchHandler.onNext(value);
  }

  @Override
  public void onRuntimeStart(final RuntimeStart value) {
    this.resourceManagerStartHandler.onNext(value);
  }

  @Override
  public void onRuntimeStop(final RuntimeStop value) {
    this.resourceManagerStopHandler.onNext(value);
  }

  @Override
  public void onResourceRelease(final ResourceReleaseEvent value) {
    this.resourceReleaseHandler.onNext(value);
  }

  @Override
  public void onResourceRequest(final ResourceRequestEvent value) {
    this.resourceRequestHandler.onNext(value);
  }
}
