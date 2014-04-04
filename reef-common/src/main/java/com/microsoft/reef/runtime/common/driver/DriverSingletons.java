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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.*;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * A class that depends on all objects we want to enforce to be singletons in the Driver.
 * The DriverRuntimeStartHandler depends on an instance of this class, which instantiates its dependencies.
 */
@DriverSide
@Private
final class DriverSingletons {
  @Inject
  DriverSingletons(final @Parameter(DriverConfigurationOptions.ActiveContextHandlers.class) Set<EventHandler<ActiveContext>> activeContextEventHandlers,
                   final @Parameter(DriverConfigurationOptions.ClosedContextHandlers.class) Set<EventHandler<ClosedContext>> closedContextEventHandlers,
                   final @Parameter(DriverConfigurationOptions.FailedContextHandlers.class) Set<EventHandler<FailedContext>> failedContextEventHandlers,
                   final @Parameter(DriverConfigurationOptions.ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
                   final @Parameter(DriverConfigurationOptions.RunningTaskHandlers.class) Set<EventHandler<RunningTask>> runningTaskEventHandlers,
                   final @Parameter(DriverConfigurationOptions.CompletedTaskHandlers.class) Set<EventHandler<CompletedTask>> completedTaskEventHandlers,
                   final @Parameter(DriverConfigurationOptions.SuspendedTaskHandlers.class) Set<EventHandler<SuspendedTask>> suspendedTaskEventHandlers,
                   final @Parameter(DriverConfigurationOptions.TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
                   final @Parameter(DriverConfigurationOptions.FailedTaskHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
                   final @Parameter(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class) Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorEventHandlers,
                   final @Parameter(DriverConfigurationOptions.FailedEvaluatorHandlers.class) Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
                   final @Parameter(DriverConfigurationOptions.CompletedEvaluatorHandlers.class) Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers) {
  }
}
