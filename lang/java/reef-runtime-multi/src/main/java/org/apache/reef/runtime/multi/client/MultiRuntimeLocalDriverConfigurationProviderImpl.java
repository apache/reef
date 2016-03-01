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
import org.apache.reef.runtime.multi.utils.avro.RuntimeDefinition;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.net.URI;
import java.util.Set;

/**
 * MultiRuntime configuration provider for Local/Local runtime.
 * This is a test provider that wraps local runtime with multi runtime.
 */
@Private
@RuntimeAuthor
public final class MultiRuntimeLocalDriverConfigurationProviderImpl extends AbstractDriverConfigurationProvider {
  private final int maxEvaluators;
  private final double jvmHeapSlack;
  private final Set<String> rackNames;

  @Inject
  private MultiRuntimeLocalDriverConfigurationProviderImpl(
          @Parameter(MaxNumberOfEvaluators.class) final int maxEvaluators,
          @Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
          @Parameter(RackNames.class) final Set<String> rackNames) {
    this.maxEvaluators = maxEvaluators;
    this.jvmHeapSlack = jvmHeapSlack;
    this.rackNames = rackNames;
  }

  @Override
  protected RuntimeDefinition[] getRuntimeDefinitions(final URI jobFolder,
                                                      final String clientRemoteId,
                                                      final String jobId) {
    ConfigurationModule configModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, this.maxEvaluators)
            .set(LocalDriverConfiguration.ROOT_FOLDER, jobFolder.toString())
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, this.jvmHeapSlack)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(LocalDriverConfiguration.JOB_IDENTIFIER, jobId);
    for (final String rackName : rackNames) {
      configModule = configModule.set(LocalDriverConfiguration.RACK_NAMES, rackName);
    }

    final RuntimeDefinition localRuntimeDefinition = createRuntimeDefinition(
            configModule,
            org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME,
            true);

    final RuntimeDefinition[] ret = {localRuntimeDefinition};
    return ret;
  }
}
