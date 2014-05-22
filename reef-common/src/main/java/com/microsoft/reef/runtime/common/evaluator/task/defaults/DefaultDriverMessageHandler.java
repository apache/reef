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
package com.microsoft.reef.runtime.common.evaluator.task.defaults;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * A default implementation of EventHandler<DriverMessage>
 */
@Private
public final class DefaultDriverMessageHandler implements EventHandler<DriverMessage> {

  @Inject
  public DefaultDriverMessageHandler() {
  }

  @Override
  public void onNext(final DriverMessage driverMessage) {
    throw new RuntimeException("No DriverMessage handler bound. Message received:" + driverMessage);
  }
}
