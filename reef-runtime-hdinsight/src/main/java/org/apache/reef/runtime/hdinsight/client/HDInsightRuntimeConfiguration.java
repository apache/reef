/**
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
package org.apache.reef.runtime.hdinsight.client;

import org.apache.reef.runtime.hdinsight.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

import java.io.File;
import java.io.IOException;

/**
 * Configuration module to setup REEF to submit jobs to HDInsight.
 */
public final class HDInsightRuntimeConfiguration extends ConfigurationModuleBuilder {

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

  /**
   * The environment variable that holds the path to the default configuration file
   */
  public static final String HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE = "REEF_HDI_CONF";

  public static final ConfigurationModule CONF = new HDInsightRuntimeConfiguration()
      .merge(HDInsightRuntimeConfigurationStatic.CONF)
      .bindNamedParameter(AzureStorageAccountName.class, STORAGE_ACCOUNT_NAME)
      .bindNamedParameter(AzureStorageAccountKey.class, STORAGE_ACCOUNT_KEY)
      .bindNamedParameter(AzureStorageAccountContainerName.class, CONTAINER_NAME)
      .bindNamedParameter(HDInsightInstanceURL.class, URL)
      .bindNamedParameter(HDInsightUsername.class, USER_NAME)
      .bindNamedParameter(HDInsightPassword.class, PASSWORD)
      .build();

  /**
   * Returns a HDInsight runtime configuration from the credentials stored in the given file.
   *
   * @param file
   * @return a HDInsight runtime configuration from the credentials stored in the given file.
   * @throws IOException if the file can't be read
   */
  public static Configuration fromTextFile(final File file) throws IOException {
    final Configuration loaded = new AvroConfigurationSerializer().fromTextFile(file);
    final Configuration staticConfiguration = HDInsightRuntimeConfigurationStatic.CONF.build();
    return Configurations.merge(loaded, staticConfiguration);
  }

  /**
   * @return the RuntimeConfiguration that is stored in a file refered to
   * by the environment variable HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE.
   * @throws IOException
   * @see HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE
   */
  public static Configuration fromEnvironment() throws IOException {

    final String configurationPath =
        System.getenv(HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE);

    if (null == configurationPath) {
      throw new IOException("Environment Variable " +
          HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " not set.");
    }

    final File configurationFile = new File(configurationPath);
    if (!configurationFile.canRead()) {
      throw new IOException("Environment Variable " +
          HDINSIGHT_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " points to a file " + configurationFile.getAbsolutePath() +
          " which can't be read."
      );
    }

    return fromTextFile(configurationFile);
  }
}
