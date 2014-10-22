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
package com.microsoft.reef.runtime.common.client.defaults;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.FailedJob;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Default event handler for FailedJob: rethrow the exception.
 */
@Provided
@ClientSide
public final class DefaultFailedJobHandler implements EventHandler<FailedJob> {

  @Inject
  private DefaultFailedJobHandler() {
  }

  @Override
  public void onNext(final FailedJob job) {
    throw new RuntimeException("REEF job failed: " + job.getId(), job.getReason().orElse(null));
  }
}
