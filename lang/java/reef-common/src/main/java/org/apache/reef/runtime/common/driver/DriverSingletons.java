/*
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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.proto.ClientRuntimeProtocol;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorIdlenessThreadPool;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

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
      @Parameter(ContextActiveHandlers.class) final Set<EventHandler<ActiveContext>> contextActiveEventHandlers,
      @Parameter(ContextClosedHandlers.class) final Set<EventHandler<ClosedContext>> contextClosedEventHandlers,
      @Parameter(ContextFailedHandlers.class) final Set<EventHandler<FailedContext>> contextFailedEventHandlers,
      @Parameter(ContextMessageHandlers.class) final Set<EventHandler<ContextMessage>> contextMessageHandlers,
      @Parameter(TaskRunningHandlers.class) final Set<EventHandler<RunningTask>> taskRunningEventHandlers,
      @Parameter(TaskCompletedHandlers.class) final Set<EventHandler<CompletedTask>> taskCompletedEventHandlers,
      @Parameter(TaskSuspendedHandlers.class) final Set<EventHandler<SuspendedTask>> taskSuspendedEventHandlers,
      @Parameter(TaskMessageHandlers.class) final Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      @Parameter(TaskFailedHandlers.class) final Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      @Parameter(EvaluatorAllocatedHandlers.class)
      final Set<EventHandler<AllocatedEvaluator>> evaluatorAllocatedEventHandlers,
      @Parameter(EvaluatorFailedHandlers.class) final Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers,
      @Parameter(EvaluatorCompletedHandlers.class)
      final Set<EventHandler<CompletedEvaluator>> evaluatorCompletedHandlers,

      // Service event handlers
      @Parameter(ServiceContextActiveHandlers.class)
      final Set<EventHandler<ActiveContext>> serviceContextActiveEventHandlers,
      @Parameter(ServiceContextClosedHandlers.class)
      final Set<EventHandler<ClosedContext>> serviceContextClosedEventHandlers,
      @Parameter(ServiceContextFailedHandlers.class)
      final Set<EventHandler<FailedContext>> serviceContextFailedEventHandlers,
      @Parameter(ServiceContextMessageHandlers.class)
      final Set<EventHandler<ContextMessage>> serviceContextMessageHandlers,
      @Parameter(ServiceTaskRunningHandlers.class)
      final Set<EventHandler<RunningTask>> serviceTaskRunningEventHandlers,
      @Parameter(ServiceTaskCompletedHandlers.class)
      final Set<EventHandler<CompletedTask>> serviceTaskCompletedEventHandlers,
      @Parameter(ServiceTaskSuspendedHandlers.class)
      final Set<EventHandler<SuspendedTask>> serviceTaskSuspendedEventHandlers,
      @Parameter(ServiceTaskMessageHandlers.class) final Set<EventHandler<TaskMessage>> serviceTaskMessageEventHandlers,
      @Parameter(ServiceTaskFailedHandlers.class) final Set<EventHandler<FailedTask>> serviceTaskExceptionEventHandlers,
      @Parameter(ServiceEvaluatorAllocatedHandlers.class)
      final Set<EventHandler<AllocatedEvaluator>> serviceEvaluatorAllocatedEventHandlers,
      @Parameter(ServiceEvaluatorFailedHandlers.class)
      final Set<EventHandler<FailedEvaluator>> serviceEvaluatorFailedHandlers,
      @Parameter(ServiceEvaluatorCompletedHandlers.class)
      final Set<EventHandler<CompletedEvaluator>> serviceEvaluatorCompletedHandlers,

      // Client event handler
      @Parameter(DriverRuntimeConfigurationOptions.JobControlHandler.class)
      final EventHandler<ClientRuntimeProtocol.JobControlProto> jobControlHandler,

      // Resource*Handlers - Should be invoked once
      // The YarnResourceLaunchHandler creates and uploads
      // the global jar file. If these handlers are
      // instantiated for each container allocation
      // we get container failures dure to modifications
      // to already submitted global jar file
      final ResourceLaunchHandler resourceLaunchHandler,
      final ResourceReleaseHandler resourceReleaseHandler,

      final EvaluatorIdlenessThreadPool evaluatorIdlenessThreadPool) {
  }
}
