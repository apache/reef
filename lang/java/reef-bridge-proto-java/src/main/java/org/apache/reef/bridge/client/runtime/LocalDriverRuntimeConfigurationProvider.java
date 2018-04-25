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
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;

/**
 * Local driver runtime configuration provider for the bridge.
 */
public final class LocalDriverRuntimeConfigurationProvider implements IDriverRuntimeConfigurationProvider {

  @Inject
  LocalDriverRuntimeConfigurationProvider() {
  }

  @Override
  public Configuration getConfiguration(final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    ConfigurationModule localRuntimeCM = LocalRuntimeConfiguration.CONF;
    if (driverConfiguration.getLocalRuntime().getMaxNumberOfEvaluators() > 0) {
      localRuntimeCM = localRuntimeCM.set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS,
          driverConfiguration.getLocalRuntime().getMaxNumberOfEvaluators());
    }
    if (StringUtils.isNotEmpty(driverConfiguration.getLocalRuntime().getRuntimeRootFolder())) {
      localRuntimeCM = localRuntimeCM.set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER,
          driverConfiguration.getLocalRuntime().getRuntimeRootFolder());
    }
    if (driverConfiguration.getLocalRuntime().getJvmHeapSlack() > 0.0) {
      localRuntimeCM = localRuntimeCM.set(LocalRuntimeConfiguration.JVM_HEAP_SLACK,
          driverConfiguration.getLocalRuntime().getJvmHeapSlack());
    }
    return localRuntimeCM.build();
  }
}
