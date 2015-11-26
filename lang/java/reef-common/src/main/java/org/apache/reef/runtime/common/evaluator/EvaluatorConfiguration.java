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
package org.apache.reef.runtime.common.evaluator;

import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.WorkingDirectoryTempFileCreator;
import org.apache.reef.runtime.common.evaluator.parameters.*;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The runtime configuration of an evaluator.
 */
public final class EvaluatorConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> DRIVER_REMOTE_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> EVALUATOR_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROOT_CONTEXT_CONFIGURATION = new RequiredParameter<>();
  public static final OptionalParameter<String> ROOT_SERVICE_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<String> TASK_CONFIGURATION = new OptionalParameter<>();
  public static final OptionalParameter<Integer> HEARTBEAT_PERIOD = new OptionalParameter<>();
  public static final OptionalParameter<String> APPLICATION_IDENTIFIER = new OptionalParameter<>();

  /**
   * The EVALUATOR_CONFIG_MODULE_BUILDER which contains bindings shared for all kinds of Evaluators.
   */
  private static final ConfigurationModuleBuilder EVALUATOR_CONFIG_MODULE_BUILDER = new EvaluatorConfiguration()
      .bindNamedParameter(DriverRemoteIdentifier.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(ErrorHandlerRID.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(EvaluatorIdentifier.class, EVALUATOR_IDENTIFIER)
      .bindNamedParameter(HeartbeatPeriod.class, HEARTBEAT_PERIOD)
      .bindNamedParameter(RootContextConfiguration.class, ROOT_CONTEXT_CONFIGURATION)
      .bindNamedParameter(InitialTaskConfiguration.class, TASK_CONFIGURATION)
      .bindNamedParameter(RootServiceConfiguration.class, ROOT_SERVICE_CONFIGURATION)
      .bindNamedParameter(ApplicationIdentifier.class, APPLICATION_IDENTIFIER)
      .bindNamedParameter(LaunchID.class, APPLICATION_IDENTIFIER);

  /**
   * This is ConfigurationModule for CLR Evaluator.
   */
  public static final ConfigurationModule CONFCLR = EVALUATOR_CONFIG_MODULE_BUILDER.build();

  /**
   * This is ConfigurationModule for Java Evaluator.
   */
  public static final ConfigurationModule CONF = EVALUATOR_CONFIG_MODULE_BUILDER
      .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)
      .bindSetEntry(Clock.RuntimeStartHandler.class, EvaluatorRuntime.RuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, EvaluatorRuntime.RuntimeStopHandler.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .build();

  private static final class ExecutorServiceConstructor implements ExternalConstructor<ExecutorService> {

    @Inject
    ExecutorServiceConstructor() {
    }

    @Override
    public ExecutorService newInstance() {
      return Executors.newCachedThreadPool();
    }
  }
}
