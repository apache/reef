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
package org.apache.reef.bridge.driver.launch.hdinsight;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.launch.RuntimeConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.runtime.hdinsight.client.HDInsightRuntimeConfiguration;
import org.apache.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;

/**
 * Azure batch runtime configuration provider.
 */
@Private
public final class HDInsightConfigurationProvider implements RuntimeConfigurationProvider {

  @Inject
  private HDInsightConfigurationProvider() {
  }

  public Configuration getRuntimeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration) {
    return generateConfigurationFromJobSubmissionParameters(driverClientConfiguration);
  }

  private static Configuration generateConfigurationFromJobSubmissionParameters(
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration) {
    if (driverClientConfiguration.getHdiRuntime().getUnsafe()) {
      return UnsafeHDInsightRuntimeConfiguration.CONF
          .set(UnsafeHDInsightRuntimeConfiguration.USER_NAME,
              driverClientConfiguration.getHdiRuntime().getHdiUserName())
          .set(UnsafeHDInsightRuntimeConfiguration.PASSWORD,
              driverClientConfiguration.getHdiRuntime().getHdiPassword())
          .set(UnsafeHDInsightRuntimeConfiguration.URL,
              driverClientConfiguration.getHdiRuntime().getHdiUrl())
          .set(UnsafeHDInsightRuntimeConfiguration.STORAGE_ACCOUNT_NAME,
              driverClientConfiguration.getHdiRuntime().getAzureStorageAccountName())
          .set(UnsafeHDInsightRuntimeConfiguration.STORAGE_ACCOUNT_KEY,
              driverClientConfiguration.getHdiRuntime().getAzureStorageAccountKey())
          .set(UnsafeHDInsightRuntimeConfiguration.CONTAINER_NAME,
              driverClientConfiguration.getHdiRuntime().getAzureStorageContainerName())
          .build();
    } else {
      return HDInsightRuntimeConfiguration.CONF
          .set(HDInsightRuntimeConfiguration.USER_NAME,
              driverClientConfiguration.getHdiRuntime().getHdiUserName())
          .set(HDInsightRuntimeConfiguration.PASSWORD,
              driverClientConfiguration.getHdiRuntime().getHdiPassword())
          .set(HDInsightRuntimeConfiguration.URL,
              driverClientConfiguration.getHdiRuntime().getHdiUrl())
          .set(HDInsightRuntimeConfiguration.STORAGE_ACCOUNT_NAME,
              driverClientConfiguration.getHdiRuntime().getAzureStorageAccountName())
          .set(HDInsightRuntimeConfiguration.STORAGE_ACCOUNT_KEY,
              driverClientConfiguration.getHdiRuntime().getAzureStorageAccountKey())
          .set(HDInsightRuntimeConfiguration.CONTAINER_NAME,
              driverClientConfiguration.getHdiRuntime().getAzureStorageContainerName())
          .build();
    }
  }
}
