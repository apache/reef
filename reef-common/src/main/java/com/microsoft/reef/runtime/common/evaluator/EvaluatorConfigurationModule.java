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
package com.microsoft.reef.runtime.common.evaluator;

import com.microsoft.reef.proto.ReefServiceProtos.TaskStatusProto;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class EvaluatorConfigurationModule extends ConfigurationModuleBuilder {

  @NamedParameter(doc = "The event handler for task manager status.")
  final static class TaskManagerStatusHandler implements Name<EventHandler<TaskStatusProto>> {
  }

  private final static class ExecutorServiceConstructor implements ExternalConstructor<ExecutorService> {

    @Inject
    ExecutorServiceConstructor() {
    }

    @Override
    public ExecutorService newInstance() {
      return Executors.newCachedThreadPool();
    }
  }

  @NamedParameter(doc = "The evaluator root context configuration.")
  public final static class RootContextConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "The service configuration for the root context")
  public final static class RootServiceConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "An initial task to launch on startup.")
  public final static class TaskConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "The remote identifier to the driver runtime.")
  public final static class DriverRemoteIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The evaluator identifier.")
  public final static class EvaluatorIdentifier implements Name<String> {
  }

  @NamedParameter(doc = "The evaluator heartbeat period.", default_value = "5000")
  public final static class HeartbeatPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "The RM application/job identifier.")
  public final static class ApplicationIdentifier implements Name<String> {
  }

  public static final RequiredParameter<String> DRIVER_REMOTE_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> EVALUATOR_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROOT_CONTEXT_CONFIGURATION = new RequiredParameter<>();
  public static final OptionalParameter<String> ROOT_SERVICE_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<String> TASK_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<Integer> HEARTBEAT_PERIOD = new OptionalParameter<>();
  public static final OptionalParameter<String> APPLICATION_IDENTIFIER = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new EvaluatorConfigurationModule()
      .bindSetEntry(Clock.RuntimeStartHandler.class, EvaluatorRuntime.RuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, EvaluatorRuntime.RuntimeStopHandler.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .bindNamedParameter(DriverRemoteIdentifier.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(EvaluatorIdentifier.class, EVALUATOR_IDENTIFIER)
      .bindNamedParameter(HeartbeatPeriod.class, HEARTBEAT_PERIOD)
      .bindNamedParameter(RootContextConfiguration.class, ROOT_CONTEXT_CONFIGURATION)
      .bindNamedParameter(TaskConfiguration.class, TASK_CONFIGURATION)
      .bindNamedParameter(RootServiceConfiguration.class, ROOT_SERVICE_CONFIGURATION)
      .bindNamedParameter(ApplicationIdentifier.class, APPLICATION_IDENTIFIER)
      .build();
}
