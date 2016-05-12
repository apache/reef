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
package org.apache.reef.driver.restart;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.exception.DriverFatalRuntimeException;

import javax.inject.Inject;
import java.util.Set;

/**
 * The default driver runtime restart manager that is not able to perform any restart actions.
 * Thus, when performing actions pertaining to restart, it is recommended to call
 * {@link DriverRuntimeRestartManager#getResubmissionAttempts()} first and check for > 0.
 */
@Private
@DriverSide
@Unstable
final class DefaultDriverRuntimeRestartMangerImpl implements DriverRuntimeRestartManager {
  @Inject
  private DefaultDriverRuntimeRestartMangerImpl() {
  }

  @Override
  public int getResubmissionAttempts() {
    return 0;
  }

  @Override
  public void recordAllocatedEvaluator(final String id) {
    throw new DriverFatalRuntimeException(
        "Restart is not enabled. recordAllocatedEvaluator should not have been called.");
  }

  @Override
  public void recordRemovedEvaluator(final String id) {
    throw new DriverFatalRuntimeException(
        "Restart is not enabled. recordRemovedEvaluator should not have been called.");
  }

  @Override
  public RestartEvaluators getPreviousEvaluators() {
    throw new DriverFatalRuntimeException(
        "Restart is not enabled. getPreviousEvaluators should not have been called.");
  }

  @Override
  public void informAboutEvaluatorFailures(final Set<String> failedEvaluatorIds) {
    throw new DriverFatalRuntimeException(
        "Restart is not enabled. informAboutEvaluatorFailures should not have been called.");
  }
}
