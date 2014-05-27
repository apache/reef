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

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.runtime.common.evaluator.task.defaults.DefaultCloseHandler;
import com.microsoft.reef.runtime.common.evaluator.task.defaults.DefaultDriverMessageHandler;
import com.microsoft.reef.runtime.common.evaluator.task.defaults.DefaultSuspendHandler;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.task.events.*;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

import java.util.Set;

/**
 * Configuration parameters for the TaskConfiguration class.
 */
@Public
@DriverSide
@Provided
public final class TaskConfigurationOptions {

  @NamedParameter(default_value = "Unnamed Task", doc = "The Identifier of the Task")
  public static final class Identifier implements Name<String> {
  }

  @NamedParameter(doc = "The memento to be used for the Task.")
  public final class Memento implements Name<String> {
  }

  @NamedParameter(doc = "TaskMessageSource instances.")
  public final class TaskMessageSources implements Name<Set<TaskMessageSource>> {
  }

  @NamedParameter(doc = "The set of event handlers for the TaskStart event.")
  public final class StartHandlers implements Name<Set<EventHandler<TaskStart>>> {
  }

  @NamedParameter(doc = "The set of event handlers for the TaskStop event.")
  public final class StopHandlers implements Name<Set<EventHandler<TaskStop>>> {
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
