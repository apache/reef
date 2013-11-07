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
package com.microsoft.reef.runtime.common.evaluator.activity;

import com.microsoft.reef.activity.events.ActivityStart;
import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * Convenience class to send activity start and stop events.
 */
final class ActivityLifeCycle {

  private final Set<EventHandler<ActivityStop>> activityStopHandlers;
  private final Set<EventHandler<ActivityStart>> activityStartHandlers;
  private final ActivityStart activityStart;
  private final ActivityStop activityStop;

  @Inject
  ActivityLifeCycle(final @Parameter(ActivityConfiguration.StopHandlers.class) Set<EventHandler<ActivityStop>> activityStopHandlers,
                    final @Parameter(ActivityConfiguration.StartHandlers.class) Set<EventHandler<ActivityStart>> activityStartHandlers,
                    final ActivityStartImpl activityStart,
                    final ActivityStopImpl activityStop) {
    this.activityStopHandlers = activityStopHandlers;
    this.activityStartHandlers = activityStartHandlers;
    this.activityStart = activityStart;
    this.activityStop = activityStop;
  }

  public void start() {
    for (final EventHandler<ActivityStart> startHandler : this.activityStartHandlers) {
      startHandler.onNext(this.activityStart);
    }
  }

  public void stop() {
    for (final EventHandler<ActivityStop> startHandler : this.activityStopHandlers) {
      startHandler.onNext(this.activityStop);
    }
  }


}
