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

package org.apache.reef.bridge.client.runtime;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.bridge.client.IDriverRuntimeConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.FileSystemUrl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;

/**
 * Yarn driver runtime configuration provider for the bridge.
 */
public final class YarnDriverRuntimeConfigurationProvider implements IDriverRuntimeConfigurationProvider {

  @Inject
  YarnDriverRuntimeConfigurationProvider() {
  }

  @Override
  public Configuration getConfiguration(final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    ConfigurationModule yarnDriverConfiguration = YarnDriverConfiguration.CONF
        .set(YarnDriverConfiguration.JOB_IDENTIFIER, driverConfiguration.getJobid())
        .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
        .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
        .set(YarnDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME);
    if (StringUtils.isNotEmpty(driverConfiguration.getYarnRuntime().getJobSubmissionDirectory())) {
      yarnDriverConfiguration = yarnDriverConfiguration
          .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY,
              driverConfiguration.getYarnRuntime().getJobSubmissionDirectory());
    }
    if (StringUtils.isNotEmpty(driverConfiguration.getYarnRuntime().getFilesystemUrl())) {
      JavaConfigurationBuilder providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(FileSystemUrl.class, driverConfiguration.getYarnRuntime().getFilesystemUrl());
      return Configurations.merge(yarnDriverConfiguration.build(), providerConfig.build());
    } else {
      return yarnDriverConfiguration.build();
    }
  }
}
