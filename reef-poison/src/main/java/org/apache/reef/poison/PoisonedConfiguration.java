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
package org.apache.reef.poison;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.poison.context.PoisonedContextStartHandler;
import org.apache.reef.poison.params.CrashProbability;
import org.apache.reef.poison.params.CrashTimeout;
import org.apache.reef.poison.task.PoisonedTaskStartHandler;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * Configure a Context with a lethal injection.
 */
public final class PoisonedConfiguration extends ConfigurationModuleBuilder {

  /**
   * The time window in seconds beginning at ContextStart during which the crash is to occur.
   */
  public static final OptionalParameter<Integer> CRASH_TIMEOUT = new OptionalParameter<>();

  /**
   * The probability with which a crash is to occur.
   */
  public static final OptionalParameter<Double> CRASH_PROBABILITY = new OptionalParameter<>();

  public static final ConfigurationModule CONTEXT_CONF = new PoisonedConfiguration()
      .bindNamedParameter(CrashTimeout.class, CRASH_TIMEOUT)
      .bindNamedParameter(CrashProbability.class, CRASH_PROBABILITY)
      .bindSetEntry(ContextStartHandlers.class, PoisonedContextStartHandler.class)
      .build();

  public static final ConfigurationModule TASK_CONF = new PoisonedConfiguration()
      .bindNamedParameter(CrashTimeout.class, CRASH_TIMEOUT)
      .bindNamedParameter(CrashProbability.class, CRASH_PROBABILITY)
      .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, PoisonedTaskStartHandler.class)
      .build();
}
