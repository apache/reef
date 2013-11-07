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


import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.activity.events.*;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultCloseHandler;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultDriverMessageHandler;
import com.microsoft.reef.runtime.common.evaluator.activity.DefaultSuspendHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;

import java.util.Set;

/**
 * A ConfigurationModule to fill out to generate legal activity Configurations that can be submitted.
 */
public class ActivityConfiguration extends ConfigurationModuleBuilder {

  /**
   * The activity to instantiate.
   */
  public static final RequiredImpl<Activity> ACTIVITY = new RequiredImpl<>();
  /**
   * Handler for activity suspension. Defaults to activity failure if not bound.
   */
  public static final OptionalImpl<EventHandler<SuspendEvent>> SUSPEND_HANDLER = new OptionalImpl<>();
  /**
   * Handler for messages from the driver. Defaults to activity failure if not bound.
   */
  public static final OptionalParameter<EventHandler<DriverMessage>> MESSAGE_HANDLER = new OptionalParameter<>();
  /**
   * Handler for closure requests from the driver. Defaults to activity failure if not bound.
   */
  public static final OptionalParameter<EventHandler<CloseEvent>> CLOSE_HANDLER = new OptionalParameter<>();
  /**
   * Identifier for the activity.
   */
  public static final RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();
  /**
   * The Base64 encoded memento to be passed to Activity.call().
   * You can do the encoding for example via DatatypeConverter.printBase64Binary()
   */
  public static final OptionalParameter<String> MEMENTO = new OptionalParameter<>();
  /**
   * Message sources invoked upon each evaluator heartbeat.
   */
  public static final OptionalImpl<ActivityMessageSource> ACTIVITY_MESSAGE_SOURCE = new OptionalImpl<>();
  /**
   * Event handler to receive ActivityStart after the Activity.call() method was called.
   */
  public static final OptionalImpl<EventHandler<ActivityStart>> ACTIVITY_START_HANDLER = new OptionalImpl<>();
  /**
   * Event handler to receive ActivityStart after the Activity.call() method returned.
   */
  public static final OptionalImpl<EventHandler<ActivityStop>> ACTIVITY_STOP_HANDLER = new OptionalImpl<>();

  /**
   * ConfigurationModule to fill out for an Activity configuration.
   */
  public static final ConfigurationModule CONF = new ActivityConfiguration()
      .bindSetEntry(ActivityMessageSources.class, ACTIVITY_MESSAGE_SOURCE)
      .bindSetEntry(StartHandlers.class, ACTIVITY_START_HANDLER)
      .bindSetEntry(StopHandlers.class, ACTIVITY_STOP_HANDLER)
      .bindNamedParameter(Identifier.class, IDENTIFIER)
      .bindNamedParameter(Memento.class, MEMENTO)
      .bindImplementation(Activity.class, ACTIVITY)
      .bindNamedParameter(CloseHandler.class, CLOSE_HANDLER)
      .bindNamedParameter(SuspendHandler.class, SUSPEND_HANDLER)
      .bindNamedParameter(MessageHandler.class, MESSAGE_HANDLER)
      .build();

  @NamedParameter(default_value = "Unnamed Activity", doc = "The Identifier of the Activity")
  public static final class Identifier implements Name<String> {// empty
  }

  @NamedParameter(doc = "The memento to be used for the Activity.")
  @Private
  public final class Memento implements Name<String> {
  }

  @NamedParameter(doc = "ActivityMessageSource instances.")
  @Private
  public final class ActivityMessageSources implements Name<Set<ActivityMessageSource>> {
  }

  @NamedParameter(doc = "The set of event handlers for the ActivityStart event.")
  @Private
  public class StartHandlers implements Name<Set<EventHandler<ActivityStart>>> {
  }

  @NamedParameter(doc = "The set of event handlers for the ActivityStop event.")
  @Private
  public class StopHandlers implements Name<Set<EventHandler<ActivityStop>>> {
  }

  @NamedParameter(doc = "The event handler that receives the close event",
      default_class = DefaultCloseHandler.class)
  @Private
  public class CloseHandler implements Name<EventHandler<CloseEvent>> {
  }

  @NamedParameter(doc = "The event handler that receives the suspend event",
      default_class = DefaultSuspendHandler.class)
  @Private
  public class SuspendHandler implements Name<EventHandler<SuspendEvent>> {
  }

  @NamedParameter(doc = "The event handler that receives messages from the driver",
      default_class = DefaultDriverMessageHandler.class)
  @Private
  public class MessageHandler implements Name<EventHandler<DriverMessage>> {
  }

}
