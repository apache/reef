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
package org.apache.reef.runime.azbatch.client;

import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.driver.AzureBatchResourceLaunchHandler;
import org.apache.reef.runime.azbatch.driver.AzureBatchResourceReleaseHandler;
import org.apache.reef.runime.azbatch.driver.AzureBatchResourceRequestHandler;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import java.net.URI;

/**
 * Configuration provider for the Azure Batch runtime.
 */
@Private
public final class AzureBatchDriverConfigurationProviderImpl implements DriverConfigurationProvider {


  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    final Configuration result = Configurations.merge(makeDriverRuntimeConfiguration(), applicationConfiguration);

    // TODO: TASK 120499
    throw new NotImplementedException();
  }

  private Configuration makeDriverRuntimeConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ResourceRequestHandler.class, AzureBatchResourceRequestHandler.class)
        .bindImplementation(ResourceLaunchHandler.class, AzureBatchResourceLaunchHandler.class)
        .bindImplementation(ResourceReleaseHandler.class, AzureBatchResourceReleaseHandler.class)
        .build();
  }
}
