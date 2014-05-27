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
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.CompletedJob;

/**
 * An implementation of CompletedJob
 */
@ClientSide
@Private
final class CompletedJobImpl implements CompletedJob {
  private final String jobId;

  CompletedJobImpl(final String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public String toString() {
    return "CompletedJob{'" + jobId + "'}";
  }
}
