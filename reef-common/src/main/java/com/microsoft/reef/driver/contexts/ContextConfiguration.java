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
package com.microsoft.reef.driver.contexts;

import com.microsoft.reef.activity.events.ActivityStart;
import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
import com.microsoft.reef.evaluator.context.ContextMessageSource;
import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.evaluator.context.events.ContextStop;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;

/**
 * A ConfigurationModule for EvaluatorContext Configuration.
 */
@Public
@DriverSide
@Provided
public class ContextConfiguration extends ConfigurationModuleBuilder {

  /**
   * The identifier of the context.
   */
  public static final RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();

  /**
   * Event handler for context start. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ContextStart>> ON_CONTEXT_STARTED = new OptionalImpl<>();

  /**
   * Event handler for context stop. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ContextStop>> ON_CONTEXT_STOP = new OptionalImpl<>();

  /**
   * Event handlers to be informed right before an Activity enters its call() method.
   */
  public static final OptionalImpl<EventHandler<ActivityStart>> ON_ACTIVITY_STARTED = new OptionalImpl<>();

  /**
   * Event handlers to be informed right after an Activity exits its call() method.
   */
  public static final OptionalImpl<EventHandler<ActivityStop>> ON_ACTIVITY_STOP = new OptionalImpl<>();

  /**
   * Source of messages to be called whenever the evaluator is about to make a heartbeat.
   */
  public static final OptionalImpl<ContextMessageSource> ON_CONTEXT_GET_MESSAGE = new OptionalImpl<>();

  /**
   * A ConfigurationModule for contexts.
   */
  public static final ConfigurationModule CONF = new ContextConfiguration()
      .bindNamedParameter(ContextConfigurationOptions.ContextIdentifier.class, IDENTIFIER)
      .bindSetEntry(ContextConfigurationOptions.StartHandlers.class, ON_CONTEXT_STARTED)
      .bindSetEntry(ContextConfigurationOptions.StopHandlers.class, ON_CONTEXT_STOP)
      .bindSetEntry(ContextConfigurationOptions.ContextMessageSources.class, ON_CONTEXT_GET_MESSAGE)
      .bindSetEntry(ActivityConfigurationOptions.StartHandlers.class, ON_ACTIVITY_STARTED)
      .bindSetEntry(ActivityConfigurationOptions.StopHandlers.class, ON_ACTIVITY_STOP)
      .build();
}
