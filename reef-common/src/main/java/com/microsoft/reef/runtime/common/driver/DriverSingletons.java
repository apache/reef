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
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.*;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
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
  DriverSingletons(
      // Application event handlers
      final @Parameter(ContextActiveHandlers.class) Set<EventHandler<ActiveContext>> contextActiveEventHandlers,
      final @Parameter(ContextClosedHandlers.class) Set<EventHandler<ClosedContext>> contextClosedEventHandlers,
      final @Parameter(ContextFailedHandlers.class) Set<EventHandler<FailedContext>> contextFailedEventHandlers,
      final @Parameter(ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
      final @Parameter(TaskRunningHandlers.class) Set<EventHandler<RunningTask>> taskRunningEventHandlers,
      final @Parameter(TaskCompletedHandlers.class) Set<EventHandler<CompletedTask>> taskCompletedEventHandlers,
      final @Parameter(TaskSuspendedHandlers.class) Set<EventHandler<SuspendedTask>> taskSuspendedEventHandlers,
      final @Parameter(TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      final @Parameter(TaskFailedHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      final @Parameter(EvaluatorAllocatedHandlers.class) Set<EventHandler<AllocatedEvaluator>> evaluatorAllocatedEventHandlers,
      final @Parameter(EvaluatorFailedHandlers.class) Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers,
      final @Parameter(EvaluatorCompletedHandlers.class) Set<EventHandler<CompletedEvaluator>> evaluatorCompletedHandlers,

      // Service event handlers
      final @Parameter(ServiceContextActiveHandlers.class) Set<EventHandler<ActiveContext>> serviceContextActiveEventHandlers,
      final @Parameter(ServiceContextClosedHandlers.class) Set<EventHandler<ClosedContext>> serviceContextClosedEventHandlers,
      final @Parameter(ServiceContextFailedHandlers.class) Set<EventHandler<FailedContext>> serviceContextFailedEventHandlers,
      final @Parameter(ServiceContextMessageHandlers.class) Set<EventHandler<ContextMessage>> serviceContextMessageHandlers,
      final @Parameter(ServiceTaskRunningHandlers.class) Set<EventHandler<RunningTask>> serviceTaskRunningEventHandlers,
      final @Parameter(ServiceTaskCompletedHandlers.class) Set<EventHandler<CompletedTask>> serviceTaskCompletedEventHandlers,
      final @Parameter(ServiceTaskSuspendedHandlers.class) Set<EventHandler<SuspendedTask>> serviceTaskSuspendedEventHandlers,
      final @Parameter(ServiceTaskMessageHandlers.class) Set<EventHandler<TaskMessage>> serviceTaskMessageEventHandlers,
      final @Parameter(ServiceTaskFailedHandlers.class) Set<EventHandler<FailedTask>> serviceTaskExceptionEventHandlers,
      final @Parameter(ServiceEvaluatorAllocatedHandlers.class) Set<EventHandler<AllocatedEvaluator>> serviceEvaluatorAllocatedEventHandlers,
      final @Parameter(ServiceEvaluatorFailedHandlers.class) Set<EventHandler<FailedEvaluator>> serviceEvaluatorFailedHandlers,
      final @Parameter(ServiceEvaluatorCompletedHandlers.class) Set<EventHandler<CompletedEvaluator>> serviceEvaluatorCompletedHandlers,

      // Client event handler
      final @Parameter(DriverRuntimeConfigurationOptions.JobControlHandler.class) EventHandler<ClientRuntimeProtocol.JobControlProto> jobControlHandler,
    
      // Resource*Handlers - Should be invoked once
      // The YarnResourceLaunchHandler creates and uploads
      // the global jar file. If these handlers are
      // instantiated for each container allocation
      // we get container failures dure to modifications
      // to already submitted global jar file
      final ResourceLaunchHandler resourceLaunchHandler,
      final ResourceReleaseHandler resourceReleaseHandler ) {
  }
}
