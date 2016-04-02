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
package org.apache.reef.runtime.multi.client;

import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.multi.driver.MultiRuntimeDriverConfiguration;
import org.apache.reef.runtime.multi.utils.MultiRuntimeDefinitionSerializer;
import org.apache.reef.runtime.multi.utils.avro.MultiRuntimeDefinition;
import org.apache.reef.runtime.multi.utils.avro.RuntimeDefinition;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.net.URI;

/**
 * Provides base class for driver configuration providers for multi runtimes.
 */
final class MultiRuntimeDriverConfigurationProvider implements DriverConfigurationProvider {
  private final MultiRuntimeDefinitionSerializer runtimeDefinitionSerializer = new MultiRuntimeDefinitionSerializer();
  private MultiRuntimeDefinitionGenerator definitionGenerator;

  @Inject
  MultiRuntimeDriverConfigurationProvider(final MultiRuntimeDefinitionGenerator definitionGenerator) {
    this.definitionGenerator = definitionGenerator;
  }

  /**
   * Assembles the driver configuration.
   *
   * @param jobFolder                The folder in which the local runtime will execute this job.
   * @param clientRemoteId           the remote identifier of the client. It is used by the Driver to establish a
   *                                 connection back to the client.
   * @param jobId                    The identifier of the job.
   * @param applicationConfiguration The configuration of the application, e.g. a filled out DriverConfiguration
   * @return The Driver configuration to be used to instantiate the Driver.
   */
  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    MultiRuntimeDefinition runtimeDefinitions = this.definitionGenerator.getMultiRuntimeDefinition(
            jobFolder,
            clientRemoteId,
            jobId);
    ConfigurationModule conf = MultiRuntimeDriverConfiguration.CONF;

    for(RuntimeDefinition runtimeDefinition : runtimeDefinitions.getRuntimes()){
      conf = conf.set(MultiRuntimeDriverConfiguration.RUNTIME_NAMES, runtimeDefinition.getRuntimeName().toString());
    }

    return Configurations.merge(applicationConfiguration,
                    conf
                    .set(MultiRuntimeDriverConfiguration.JOB_IDENTIFIER, jobId)
                    .set(MultiRuntimeDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
                    .set(MultiRuntimeDriverConfiguration.SERIALIZED_RUNTIME_DEFINITION,
                            this.runtimeDefinitionSerializer.toString(runtimeDefinitions)).build());
  }
}
