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
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
import com.microsoft.reef.evaluator.context.ContextMessageSource;
import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.evaluator.context.events.ContextStop;
import com.microsoft.reef.runtime.common.driver.contexts.defaults.DefaultContextMessageSource;
import com.microsoft.reef.runtime.common.driver.contexts.defaults.DefaultContextStartHandler;
import com.microsoft.reef.runtime.common.driver.contexts.defaults.DefaultContextStopHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;

import java.util.Set;

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
  public static final OptionalParameter<EventHandler<ContextStart>> CONTEXT_START_HANDLER = new OptionalParameter<>();

  /**
   * Event handler for context stop. Defaults to logging if not bound.
   */
  public static final OptionalParameter<EventHandler<ContextStop>> CONTEXT_STOP_HANDLER = new OptionalParameter<>();

  /**
   * Event handlers to be informed right before an Activity enters its call() method.
   */
  public static final OptionalImpl<EventHandler<ActivityStart>> ACTIVITY_START_HANDLER = new OptionalImpl<>();
  /**
   * Event handlers to be informed right after an Activity exits its call() method.
   */
  public static final OptionalImpl<EventHandler<ActivityStop>> ACTIVITY_STOP_HANDLER = new OptionalImpl<>();

  /**
   * Source of messages to be called whenever the evaluator is about to make a heartbeat.
   */
  public static final OptionalImpl<ContextMessageSource> CONTEXT_MESSAGE_SOURCE = new OptionalImpl<>();

  /**
   * A ConfigurationModule for contexts.
   */
  public static final ConfigurationModule CONF = new ContextConfiguration()
      .bindSetEntry(StartHandlers.class, CONTEXT_START_HANDLER)
      .bindSetEntry(StopHandlers.class, CONTEXT_STOP_HANDLER)
      .bindSetEntry(ActivityConfigurationOptions.StartHandlers.class, ACTIVITY_START_HANDLER)
      .bindSetEntry(ActivityConfigurationOptions.StopHandlers.class, ACTIVITY_STOP_HANDLER)
      .bindSetEntry(ContextMessageSources.class, CONTEXT_MESSAGE_SOURCE)
      .bindNamedParameter(ContextIdentifier.class, IDENTIFIER)
      .build();

  @NamedParameter(doc = "The set of event handlers for the ContextStart event.", default_classes = DefaultContextStartHandler.class)
  @Private
  public class StartHandlers implements Name<Set<EventHandler<ContextStart>>> {
  }


  @NamedParameter(doc = "The set of event handlers for the ContextStop event.", default_classes = DefaultContextStopHandler.class)
  @Private
  public class StopHandlers implements Name<Set<EventHandler<ContextStop>>> {
  }


  @NamedParameter(doc = "The set of ContextMessageSource implementations called during heartbeats.", default_classes = DefaultContextMessageSource.class)
  @Private
  public class ContextMessageSources implements Name<Set<ContextMessageSource>> {
  }

  @NamedParameter(doc = "The identifier for the context.")
  @Private
  public class ContextIdentifier implements Name<String> {
  }
}


