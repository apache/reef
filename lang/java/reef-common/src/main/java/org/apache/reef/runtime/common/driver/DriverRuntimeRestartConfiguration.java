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

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.restart.*;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;

/**
 * The base configuration module for driver restart configurations of all runtimes.
 * <p>
 */
@Private
@ClientSide
public final class DriverRuntimeRestartConfiguration extends ConfigurationModuleBuilder {

  private DriverRuntimeRestartConfiguration() {
  }

  public static final ConfigurationModule CONF = new DriverRuntimeRestartConfiguration()

      // Automatically sets preserve evaluators to true.
      .bindNamedParameter(ResourceManagerPreserveEvaluators.class, Boolean.toString(true))

      .bindSetEntry(DriverIdleSources.class, DriverRestartManager.class)
      .bindSetEntry(ServiceEvaluatorAllocatedHandlers.class, EvaluatorPreservingEvaluatorAllocatedHandler.class)
      .bindSetEntry(ServiceEvaluatorFailedHandlers.class, EvaluatorPreservingEvaluatorFailedHandler.class)
      .bindSetEntry(ServiceEvaluatorCompletedHandlers.class, EvaluatorPreservingEvaluatorCompletedHandler.class)
      .build();
}
