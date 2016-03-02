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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.multi.client.parameters.DefaultRuntimeName;
import org.apache.reef.runtime.multi.client.parameters.RuntimeNames;
import org.apache.reef.runtime.multi.utils.avro.MultiRuntimeDefinition;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * MultiRuntime configuration provider.
 */
@Private
@RuntimeAuthor
final class MultiRuntimeDefinitionGeneratorImpl implements MultiRuntimeDefinitionGenerator {
  private final double jvmSlack;
  private final int maxEvaluators;
  private final Set<String> rackNames;
  private final Map<String, ConfigurationModuleBuilder> configModulesCreators = new HashMap<>();
  private final String defaultRuntimeName;
  private final Set<String> runtimeNames;

  @Inject
  private MultiRuntimeDefinitionGeneratorImpl(
          @Parameter(JVMHeapSlack.class) final double jvmSlack,
          @Parameter(MaxNumberOfEvaluators.class) final int maxEvaluators,
          @Parameter(RackNames.class) final Set<String> rackNames,
          @Parameter(RuntimeNames.class) final Set<String> runtimeNames,
          @Parameter(DefaultRuntimeName.class) final String defaultRuntimeName) {
    this.jvmSlack = jvmSlack;
    this.maxEvaluators = maxEvaluators;
    this.rackNames = rackNames;

    Validate.notNull(runtimeNames, "Runtimes should contain at least one element");
    Validate.notEmpty(runtimeNames, "Runtimes should contain at least one element");
    Validate.isTrue(
            !StringUtils.isEmpty(defaultRuntimeName) && !StringUtils.isBlank(defaultRuntimeName),
            "Default runtime name should not be empty");
    Validate.isTrue(runtimeNames.contains(defaultRuntimeName), String.format("No runtime found for default runtime " +
            "name %s. Defined runtimes %s", defaultRuntimeName, StringUtils.join(runtimeNames, ",")));

    this.runtimeNames = runtimeNames;
    this.defaultRuntimeName = defaultRuntimeName;

    this.configModulesCreators.put(RuntimeIdentifier.RUNTIME_NAME, new ConfigurationModuleBuilder() {
        @Override
        public ConfigurationModule getConfigurationModule(final URI jobFolder,
                                                          final String clientRemoteId,
                                                          final String jobId) {
          return getYarnConfigurationModule(jobFolder, clientRemoteId, jobId);
        }
      }
    );

    this.configModulesCreators.put(
            org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME,
            new ConfigurationModuleBuilder() {
        @Override
        public ConfigurationModule getConfigurationModule(final URI jobFolder,
                                                          final String clientRemoteId,
                                                          final String jobId) {
          return getLocalConfigurationModule(jobFolder, clientRemoteId, jobId);
        }
      }
    );

  }

  private ConfigurationModule getYarnConfigurationModule(final URI jobFolder,
                                                         final String clientRemoteId,
                                                         final String jobId) {
    return YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobFolder.toString())
            .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack);

  }

  private ConfigurationModule getLocalConfigurationModule(final URI jobFolder,
                                                          final String clientRemoteId,
                                                          final String jobId) {

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

    return localModule;
  }


  public MultiRuntimeDefinition getMultiRuntimeDefinition(final URI jobFolder,
                                                          final String clientRemoteId,
                                                          final String jobId) {


    MultiRuntimeDefinitionBuilder builder = new MultiRuntimeDefinitionBuilder();
    for (final String runtime : this.runtimeNames) {
      builder.addRuntime(
              this.configModulesCreators.get(runtime).getConfigurationModule(jobFolder, clientRemoteId, jobId),
              runtime);
    }

    return builder.setDefaultRuntimeName(this.defaultRuntimeName).build();
  }

  private interface ConfigurationModuleBuilder {
    ConfigurationModule getConfigurationModule(final URI jobFolder,
                                               final String clientRemoteId,
                                               final String jobId);
  }
}
