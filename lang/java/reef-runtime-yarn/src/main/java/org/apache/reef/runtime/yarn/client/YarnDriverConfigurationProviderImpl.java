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
package org.apache.reef.runtime.yarn.client;

import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.net.URI;

import static org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration.*;

/**
 * Default driver configuration provider for yarn runtime.
 */
final class YarnDriverConfigurationProviderImpl implements DriverConfigurationProvider {
  private final double jvmSlack;

  @Inject
  YarnDriverConfigurationProviderImpl(@Parameter(JVMHeapSlack.class) final double jvmSlack) {
    this.jvmSlack = jvmSlack;
  }

  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(
      org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration.CONF
              .set(JOB_SUBMISSION_DIRECTORY, jobFolder.toString())
              .set(JOB_IDENTIFIER, jobId)
              .set(CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
              .set(JVM_HEAP_SLACK, this.jvmSlack)
              .set(RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
              .build(),
              applicationConfiguration);
  }
}
