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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.util.Optional;

final class FailedActivityImpl implements FailedActivity {

  private final Optional<ActiveContext> context;
  private final Optional<Throwable> reason;
  private final String id;

  FailedActivityImpl(final Optional<ActiveContext> context, final Throwable reason, final String id1) {
    this.context = context;
    this.reason = Optional.ofNullable(reason);
    this.id = id1;
  }

  @Override
  public Optional<ActiveContext> getActiveContext() {
    return this.context;
  }

  @Override
  public Optional<Throwable> getReason() {
    return this.reason;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return "FailedActivity{ID='" + getId() + "'}";
  }
}
