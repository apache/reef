/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.driver.context;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.ContextMessageHandler;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.evaluator.context.parameters.*;
import org.apache.reef.runtime.common.evaluator.DefaultDriverConnection;
import org.apache.reef.runtime.common.evaluator.DriverConnection;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;

/**
 * A ConfigurationModule for Context Configuration.
 */
@Public
@DriverSide
@Provided
public class ContextConfiguration extends ConfigurationModuleBuilder {

  /**
   * The identifier of the Context.
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
   * Event handlers to be informed right before a Task enters its call() method.
   */
  public static final OptionalImpl<EventHandler<TaskStart>> ON_TASK_STARTED = new OptionalImpl<>();

  /**
   * Event handlers to be informed right after a Task exits its call() method.
   */
  public static final OptionalImpl<EventHandler<TaskStop>> ON_TASK_STOP = new OptionalImpl<>();

  /**
   * Source of messages to be called whenever the evaluator is about to make a heartbeat.
   */
  public static final OptionalImpl<ContextMessageSource> ON_SEND_MESSAGE = new OptionalImpl<>();

  /**
   * Driver has sent the context a message, and this parameter is used to register a handler
   * on the context for processing that message.
   */
  public static final OptionalImpl<ContextMessageHandler> ON_MESSAGE = new OptionalImpl<>();

  /**
   * Implementation for reconnecting to driver after driver restart
   */
  public static final OptionalImpl<DriverConnection> ON_DRIVER_RECONNECT = new OptionalImpl<>();

  /**
   * A ConfigurationModule for context.
   */
  public static final ConfigurationModule CONF = new ContextConfiguration()
      .bindNamedParameter(ContextIdentifier.class, IDENTIFIER)
      .bindNamedParameter(DriverReconnect.class, ON_DRIVER_RECONNECT)
      .bindSetEntry(ContextStartHandlers.class, ON_CONTEXT_STARTED)
      .bindSetEntry(ContextStopHandlers.class, ON_CONTEXT_STOP)
      .bindSetEntry(ContextMessageSources.class, ON_SEND_MESSAGE)
      .bindSetEntry(ContextMessageHandlers.class, ON_MESSAGE)
      .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, ON_TASK_STARTED)
      .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, ON_TASK_STOP)
      .build();

  @NamedParameter(doc = "House the implementation for re-connecting to driver after driver restart",
      default_classes = DefaultDriverConnection.class)
  public static final class DriverReconnect implements Name<DriverConnection> {
  }
}
