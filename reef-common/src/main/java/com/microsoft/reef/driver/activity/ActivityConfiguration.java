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
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;

/**
 * A ConfigurationModule to fill out to generate legal activity Configurations that can be submitted.
 */
public class ActivityConfiguration extends ConfigurationModuleBuilder {

  /**
   * Identifier for the activity.
   */
  public static final RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();

  /**
   * The activity to instantiate.
   */
  public static final RequiredImpl<Activity> ACTIVITY = new RequiredImpl<>();

  /**
   * Handler for activity suspension. Defaults to activity failure if not bound.
   */
  public static final OptionalImpl<EventHandler<SuspendEvent>> ON_SUSPEND = new OptionalImpl<>();

  /**
   * Handler for messages from the driver. Defaults to activity failure if not bound.
   */
  public static final OptionalImpl<EventHandler<DriverMessage>> ON_MESSAGE = new OptionalImpl<>();

  /**
   * Handler for closure requests from the driver. Defaults to activity failure if not bound.
   */
  public static final OptionalImpl<EventHandler<CloseEvent>> ON_CLOSE = new OptionalImpl<>();

  /**
   * The Base64 encoded memento to be passed to Activity.call().
   * You can do the encoding for example via DatatypeConverter.printBase64Binary()
   */
  public static final OptionalParameter<String> MEMENTO = new OptionalParameter<>();

  /**
   * Message source invoked upon each evaluator heartbeat.
   */
  public static final OptionalImpl<ActivityMessageSource> ON_SEND_MESSAGE = new OptionalImpl<>();

  /**
   * Event handler to receive ActivityStart after the Activity.call() method was called.
   */
  public static final OptionalImpl<EventHandler<ActivityStart>> ON_ACTIVITY_STARTED = new OptionalImpl<>();

  /**
   * Event handler to receive ActivityStart after the Activity.call() method returned.
   */
  public static final OptionalImpl<EventHandler<ActivityStop>> ON_ACTIVITY_STOP = new OptionalImpl<>();

  /**
   * ConfigurationModule to fill out for an Activity configuration.
   */
  public static final ConfigurationModule CONF = new ActivityConfiguration()
      .bindNamedParameter(ActivityConfigurationOptions.Identifier.class, IDENTIFIER)
      .bindImplementation(Activity.class, ACTIVITY)
      .bindNamedParameter(ActivityConfigurationOptions.Memento.class, MEMENTO)
      .bindNamedParameter(ActivityConfigurationOptions.CloseHandler.class, ON_CLOSE)
      .bindNamedParameter(ActivityConfigurationOptions.SuspendHandler.class, ON_SUSPEND)
      .bindNamedParameter(ActivityConfigurationOptions.MessageHandler.class, ON_MESSAGE)
      .bindSetEntry(ActivityConfigurationOptions.ActivityMessageSources.class, ON_SEND_MESSAGE)
      .bindSetEntry(ActivityConfigurationOptions.StartHandlers.class, ON_ACTIVITY_STARTED)
      .bindSetEntry(ActivityConfigurationOptions.StopHandlers.class, ON_ACTIVITY_STOP)
      .build();
}
