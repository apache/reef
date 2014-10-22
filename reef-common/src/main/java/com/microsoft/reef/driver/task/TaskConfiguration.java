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
package com.microsoft.reef.driver.task;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.task.events.*;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;

/**
 * A ConfigurationModule to fill out to generate legal task Configurations that can be submitted.
 */
public class TaskConfiguration extends ConfigurationModuleBuilder {

  /**
   * Identifier for the task.
   */
  public static final RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();

  /**
   * The task to instantiate.
   */
  public static final RequiredImpl<Task> TASK = new RequiredImpl<>();

  /**
   * Handler for task suspension. Defaults to task failure if not bound.
   */
  public static final OptionalImpl<EventHandler<SuspendEvent>> ON_SUSPEND = new OptionalImpl<>();

  /**
   * Handler for messages from the driver. Defaults to task failure if not bound.
   */
  public static final OptionalImpl<EventHandler<DriverMessage>> ON_MESSAGE = new OptionalImpl<>();

  /**
   * Handler for closure requests from the driver. Defaults to task failure if not bound.
   */
  public static final OptionalImpl<EventHandler<CloseEvent>> ON_CLOSE = new OptionalImpl<>();

  /**
   * The Base64 encoded memento to be passed to Task.call().
   * You can do the encoding for example via DatatypeConverter.printBase64Binary()
   */
  public static final OptionalParameter<String> MEMENTO = new OptionalParameter<>();

  /**
   * Message source invoked upon each evaluator heartbeat.
   */
  public static final OptionalImpl<TaskMessageSource> ON_SEND_MESSAGE = new OptionalImpl<>();

  /**
   * Event handler to receive TaskStart after the Task.call() method was called.
   */
  public static final OptionalImpl<EventHandler<TaskStart>> ON_TASK_STARTED = new OptionalImpl<>();

  /**
   * Event handler to receive TaskStop after the Task.call() method returned.
   */
  public static final OptionalImpl<EventHandler<TaskStop>> ON_TASK_STOP = new OptionalImpl<>();

  /**
   * ConfigurationModule to fill out for a Task configuration.
   */
  public static final ConfigurationModule CONF = new TaskConfiguration()
      .bindNamedParameter(TaskConfigurationOptions.Identifier.class, IDENTIFIER)
      .bindImplementation(Task.class, TASK)
      .bindNamedParameter(TaskConfigurationOptions.Memento.class, MEMENTO)
      .bindNamedParameter(TaskConfigurationOptions.CloseHandler.class, ON_CLOSE)
      .bindNamedParameter(TaskConfigurationOptions.SuspendHandler.class, ON_SUSPEND)
      .bindNamedParameter(TaskConfigurationOptions.MessageHandler.class, ON_MESSAGE)
      .bindSetEntry(TaskConfigurationOptions.TaskMessageSources.class, ON_SEND_MESSAGE)
      .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, ON_TASK_STARTED)
      .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, ON_TASK_STOP)
      .build();
}
