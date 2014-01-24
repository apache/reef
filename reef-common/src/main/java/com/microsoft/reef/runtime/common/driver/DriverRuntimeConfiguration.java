/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.wake.time.Clock;

@Private
@ClientSide
public final class DriverRuntimeConfiguration extends ConfigurationModuleBuilder {

  public static final ConfigurationModule CONF = new DriverRuntimeConfiguration()
      // Resource Catalog
      .bindImplementation(ResourceCatalog.class, ResourceCatalogImpl.class)

          // JobMessageObserver
      .bindImplementation(EvaluatorRequestor.class, DriverManager.class) // requesting evaluators
      .bindImplementation(JobMessageObserver.class, ClientJobStatusHandler.class) // sending message to job client

          // JobMessageObserver Wake event handler bindings
      .bindNamedParameter(DriverRuntimeConfigurationOptions.JobMessageHandler.class, ClientJobStatusHandler.JobMessageHandler.class)
      .bindNamedParameter(DriverRuntimeConfigurationOptions.JobExceptionHandler.class, ClientJobStatusHandler.JobExceptionHandler.class)

          // Client manager
      .bindNamedParameter(DriverRuntimeConfigurationOptions.JobControlHandler.class, ClientManager.class)

          // Bind the runtime parameters
      .bindNamedParameter(RuntimeParameters.NodeDescriptorHandler.class, DriverManager.NodeDescriptorHandler.class)
      .bindNamedParameter(RuntimeParameters.ResourceAllocationHandler.class, DriverManager.ResourceAllocationHandler.class)
      .bindNamedParameter(RuntimeParameters.ResourceStatusHandler.class, DriverManager.ResourceStatusHandler.class)
      .bindNamedParameter(RuntimeParameters.RuntimeStatusHandler.class, DriverManager.RuntimeStatusHandler.class)

          // Bind to the Clock
      .bindSetEntry(Clock.RuntimeStartHandler.class, DriverManager.RuntimeStartHandler.class)
      .bindSetEntry(Clock.StartHandler.class, ClientJobStatusHandler.StartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, DriverManager.RuntimeStopHandler.class)
      .bindSetEntry(Clock.IdleHandler.class, DriverManager.IdleHandler.class)

      .build();
}
