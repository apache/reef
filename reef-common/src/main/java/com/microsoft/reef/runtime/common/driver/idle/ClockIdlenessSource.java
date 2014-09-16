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

import com.microsoft.tang.InjectionFuture;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.runtime.event.IdleClock;

import javax.inject.Inject;

/**
 * Informs the DriverIdleManager of clock idleness.
 */
public final class ClockIdlenessSource implements DriverIdlenessSource, EventHandler<IdleClock> {
  private static final String COMPONENT_NAME = "Clock";
  private static final String IDLE_REASON = "The clock reported idle.";
  private static final String NOT_IDLE_REASON = "The clock reported not idle.";
  private static final IdleMessage IDLE_MESSAGE = new IdleMessage(COMPONENT_NAME, IDLE_REASON, true);
  private static final IdleMessage NOT_IDLE_MESSAGE = new IdleMessage(COMPONENT_NAME, NOT_IDLE_REASON, false);

  private final InjectionFuture<DriverIdleManager> driverIdleManager;
  private final Clock clock;

  @Inject
  ClockIdlenessSource(final InjectionFuture<DriverIdleManager> driverIdleManager, final Clock clock) {
    this.driverIdleManager = driverIdleManager;
    this.clock = clock;
  }

  @Override
  public synchronized IdleMessage getIdleStatus() {
    if (this.clock.isIdle()) {
      return IDLE_MESSAGE;
    } else {
      return NOT_IDLE_MESSAGE;
    }
  }

  @Override
  public synchronized void onNext(final IdleClock idleClock) {
    this.driverIdleManager.get().onPotentiallyIdle(IDLE_MESSAGE);
  }
}
