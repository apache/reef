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

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.net.URI;
import java.util.Set;

/**
 * MultiRuntime configuration provider for Yarn/Local runtime.
 */
@Private
@RuntimeAuthor
public final class MultiRuntimeYarnLocalDriverConfigurationProviderImpl extends AbstractDriverConfigurationProvider {
  private final double jvmSlack;
  private final int maxEvaluators;
  private final Set<String> rackNames;

  @Inject
  private MultiRuntimeYarnLocalDriverConfigurationProviderImpl(
          @Parameter(JVMHeapSlack.class) final double jvmSlack,
          @Parameter(MaxNumberOfEvaluators.class) final int maxEvaluators,
          @Parameter(RackNames.class) final Set<String> rackNames) {
    this.jvmSlack = jvmSlack;
    this.maxEvaluators = maxEvaluators;
    this.rackNames = rackNames;
  }

  @Override
  protected String[] getDriverConfiguration(final URI jobFolder,
                                            final String clientRemoteId,
                                            final String jobId) {

    ConfigurationModule yarnModule = YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobFolder.toString())
            .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack);
    final String serializedYarnConfig = serializeConfiguration(yarnModule);
    final String serializedYarnConfiguration = serializeRuntimeDefinition(
            serializedYarnConfig,
            true,
            org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME);

    ConfigurationModule localModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, this.maxEvaluators)
            // ROOT FOLDER will point to the current runtime directory
            .set(LocalDriverConfiguration.ROOT_FOLDER, ".")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, jobId);
    for (final String rackName : rackNames) {
      localModule = localModule.set(LocalDriverConfiguration.RACK_NAMES, rackName);
    }

    final String serializedLocalConfig = serializeConfiguration(localModule);
    final String serializedLocalConfiguration = serializeRuntimeDefinition(
            serializedLocalConfig,
            false,
            org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME);
    return new String[]{serializedYarnConfiguration, serializedLocalConfiguration};
  }
}
