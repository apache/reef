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
package com.microsoft.reef.runtime.common.driver.idle;

import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * Checks for idleness of the Event handlers.
 */
public final class EventHandlerIdlenessSource implements DriverIdlenessSource {

  private static final IdleMessage IDLE_MESSAGE = new IdleMessage("EventHandlers", "All events have been processed.", true);
  private static final IdleMessage NOT_IDLE_MESSAGE = new IdleMessage("EventHandlers", "Some events are still in flight.", true);

  private final InjectionFuture<Evaluators> evaluators;
  private final InjectionFuture<DriverIdleManager> driverIdleManager;

  @Inject
  EventHandlerIdlenessSource(final InjectionFuture<Evaluators> evaluators,
                             final InjectionFuture<DriverIdleManager> driverIdleManager) {
    this.evaluators = evaluators;
    this.driverIdleManager = driverIdleManager;
  }


  @Override
  public IdleMessage getIdleStatus() {
    if (this.evaluators.get().allEvaluatorsAreClosed()) {
      return IDLE_MESSAGE;
    } else {
      return NOT_IDLE_MESSAGE;
    }
  }

  public void check() {
    if (this.evaluators.get().allEvaluatorsAreClosed()) {
      this.driverIdleManager.get().onPotentiallyIdle(IDLE_MESSAGE);
    }
  }
}
