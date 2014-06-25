/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.examples.hello;

import com.microsoft.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * HelloREEF running on HDInsight
 */
public class HelloHDInsight {

  private static Configuration getRuntimeConfiguration(final String[] args) {

    final String username = args[0];
    final String password = args[1];
    final String url = args[2];
    final String storageAccount = args[3];
    final String containerName = args[4];
    final String storageKey = args[5];

    return UnsafeHDInsightRuntimeConfiguration.CONF
        .set(UnsafeHDInsightRuntimeConfiguration.USER_NAME, username)
        .set(UnsafeHDInsightRuntimeConfiguration.PASSWORD, password)
        .set(UnsafeHDInsightRuntimeConfiguration.URL, url)
        .set(UnsafeHDInsightRuntimeConfiguration.STORAGE_ACCOUNT_NAME, storageAccount)
        .set(UnsafeHDInsightRuntimeConfiguration.CONTAINER_NAME, containerName)
        .set(UnsafeHDInsightRuntimeConfiguration.STORAGE_ACCOUNT_KEY, storageKey)
        .build();
  }

  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = getRuntimeConfiguration(args);
    HelloREEFNoClient.runHelloReefWithoutClient(runtimeConfiguration);
  }
}
