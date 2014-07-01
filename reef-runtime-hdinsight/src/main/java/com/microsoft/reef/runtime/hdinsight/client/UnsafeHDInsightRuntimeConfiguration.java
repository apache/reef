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
package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.runtime.hdinsight.parameters.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;

import java.io.File;
import java.io.IOException;

/**
 * Same as HDInsightRuntimeConfiguration, but ignores SSL errors on submission.
 */
public final class UnsafeHDInsightRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The URL of the hdinsight web service. E.g. http://services.mycompany.com:1234/templeton/v1/
   */
  public static final RequiredParameter<String> URL = new RequiredParameter<>();

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> STORAGE_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Storage account key to be used by Azure.
   */
  public static final RequiredParameter<String> STORAGE_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Container name to be used by Azure.
   */
  public static final RequiredParameter<String> CONTAINER_NAME = new RequiredParameter<>();

  /**
   * The username to be used for connecting to hdinsight.
   */
  public static final RequiredParameter<String> USER_NAME = new RequiredParameter<>();

  /**
   * The password to be used for connecting to hdinsight.
   */
  public static final RequiredParameter<String> PASSWORD = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new UnsafeHDInsightRuntimeConfiguration()
      .merge(UnsafeHDInsightRuntimeConfigurationStatic.CONF)
      .bindNamedParameter(AzureStorageAccountName.class, STORAGE_ACCOUNT_NAME)
      .bindNamedParameter(AzureStorageAccountKey.class, STORAGE_ACCOUNT_KEY)
      .bindNamedParameter(AzureStorageAccountContainerName.class, CONTAINER_NAME)
      .bindNamedParameter(HDInsightInstanceURL.class, URL)
      .bindNamedParameter(HDInsightUsername.class, USER_NAME)
      .bindNamedParameter(HDInsightPassword.class, PASSWORD)
      .build();

  /**
   * Returns an UNSAFE HDInsight runtime configuration from the credentials stored in the given file.
   *
   * @param file
   * @return an UNSAFE HDInsight runtime configuration from the credentials stored in the given file.
   * @throws java.io.IOException if the file can't be read
   */
  public static Configuration fromTextFile(final File file) throws IOException {
    final Configuration loaded = new AvroConfigurationSerializer().fromTextFile(file);
    final Configuration staticConfiguration = UnsafeHDInsightRuntimeConfigurationStatic.CONF.build();
    return Configurations.merge(loaded, staticConfiguration);
  }

  /**
   * @return the RuntimeConfiguration that is stored in a file refered to by the environment
   * variable HDInsightRuntimeConfiguration.HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE.
   * @throws IOException
   * @see HDInsightRuntimeConfiguration.HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE
   */
  public static Configuration fromEnvironment() throws IOException {

    final String configurationPath = System.getenv(
        HDInsightRuntimeConfiguration.HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE);

    if (null == configurationPath) {
      throw new IOException("Environment Variable " +
          HDInsightRuntimeConfiguration.HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " not set.");
    }

    final File configurationFile = new File(configurationPath);
    if (!configurationFile.canRead()) {
      throw new IOException("Environment Variable " +
          HDInsightRuntimeConfiguration.HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " points to a file " + configurationFile.getAbsolutePath() +
          " which can't be read."
      );
    }

    return fromTextFile(configurationFile);
  }
}
