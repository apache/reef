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
package com.microsoft.reef.driver.activity;

import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.activity.events.*;
import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultCloseHandler;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultDriverMessageHandler;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultSuspendHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

import java.util.Set;

/**
 * Configuration parameters for the ActivityConfiguration class.
 */
@Public
@DriverSide
@Provided
public final class ActivityConfigurationOptions {

  @NamedParameter(default_value = "Unnamed Activity", doc = "The Identifier of the Activity")
  public static final class Identifier implements Name<String> {
  }

  @NamedParameter(doc = "The memento to be used for the Activity.")
  public final class Memento implements Name<String> {
  }

  @NamedParameter(doc = "ActivityMessageSource instances.")
  public final class ActivityMessageSources implements Name<Set<ActivityMessageSource>> {
  }

  @NamedParameter(doc = "The set of event handlers for the ActivityStart event.")
  public final class StartHandlers implements Name<Set<EventHandler<ActivityStart>>> {
  }

  @NamedParameter(doc = "The set of event handlers for the ActivityStop event.")
  public final class StopHandlers implements Name<Set<EventHandler<ActivityStop>>> {
  }

  @NamedParameter(doc = "The event handler that receives the close event",
      default_class = DefaultCloseHandler.class)
  public final class CloseHandler implements Name<EventHandler<CloseEvent>> {
  }

  @NamedParameter(doc = "The event handler that receives the suspend event",
      default_class = DefaultSuspendHandler.class)
  public final class SuspendHandler implements Name<EventHandler<SuspendEvent>> {
  }

  @NamedParameter(doc = "The event handler that receives messages from the driver",
      default_class = DefaultDriverMessageHandler.class)
  public final class MessageHandler implements Name<EventHandler<DriverMessage>> {
  }
}
