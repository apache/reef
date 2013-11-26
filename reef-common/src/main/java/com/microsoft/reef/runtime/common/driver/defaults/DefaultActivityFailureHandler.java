/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver.defaults;

import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Default event handler used for FailedActivity: It crashes the driver.
 */
public final class DefaultActivityFailureHandler implements EventHandler<FailedActivity> {

  @Inject
  public DefaultActivityFailureHandler() {
  }

  @Override
  public void onNext(final FailedActivity failedActivity) {
    throw new RuntimeException(
        "No handler bound for FailedActivity: " + failedActivity,
        failedActivity.getCause().orElse(null));
  }
}
