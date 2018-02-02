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
package org.apache.reef.runime.azbatch.client;

import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runime.azbatch.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

import java.io.File;
import java.io.IOException;

/**
 * Configuration Module for the Azure Batch runtime.
 */
@Public
public class AzureBatchRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The Azure Batch account URI to be used by REEF.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_URI = new RequiredParameter<>();

  /**
   * The Azure Batch account name to be used by REEF.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Azure Batch account key.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Azure Batch Pool ID.
   */
  public static final RequiredParameter<String> AZURE_BATCH_POOL_ID = new RequiredParameter<>();

  /**
   * The environment variable that holds the path to the default configuration file.
   */
  public static final String AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE = "REEF_AZBATCH_CONF";

  /**
   * The name of the Azure Storage account.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The key of the Azure Storage account.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The name of the Azure Storage account container.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_CONTAINER_NAME = new RequiredParameter<>();

  /**
   * The ConfigurationModule for the local resourcemanager.
   */
  public static final ConfigurationModule CONF = new AzureBatchRuntimeConfiguration()
      .merge(AzureBatchRuntimeConfigurationStatic.CONF)
      .bindNamedParameter(AzureBatchAccountUri.class, AZURE_BATCH_ACCOUNT_URI)
      .bindNamedParameter(AzureBatchAccountName.class, AZURE_BATCH_ACCOUNT_NAME)
      .bindNamedParameter(AzureBatchAccountKey.class, AZURE_BATCH_ACCOUNT_KEY)
      .bindNamedParameter(AzureBatchPoolId.class, AZURE_BATCH_POOL_ID)
      .bindNamedParameter(AzureStorageAccountName.class, AZURE_STORAGE_ACCOUNT_NAME)
      .bindNamedParameter(AzureStorageAccountKey.class, AZURE_STORAGE_ACCOUNT_KEY)
      .bindNamedParameter(AzureStorageContainerName.class, AZURE_STORAGE_CONTAINER_NAME)
      .build();

  /**
   * Returns an Azure Batch runtime configuration from the credentials stored in the given file.
   *
   * @param file
   * @return an Azure Batch runtime configuration from the credentials stored in the given file.
   * @throws IOException if the file can't be read
   */
  public static Configuration fromTextFile(final File file) throws IOException {
    return new AvroConfigurationSerializer().fromTextFile(file);
  }

  /**
   * @return the RuntimeConfiguration that is stored in a file refered to
   * by the environment variable AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE.
   * @throws IOException
   * @see AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE
   */
  public static Configuration fromEnvironment() throws IOException {

    final String configurationPath =
        System.getenv(AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE);

    if (null == configurationPath) {
      throw new IOException("Environment Variable " +
          AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " not set.");
    }

    final File configurationFile = new File(configurationPath);
    if (!configurationFile.canRead()) {
      throw new IOException("Environment Variable " +
          AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE +
          " points to a file " + configurationFile.getAbsolutePath() +
          " which can't be read."
      );
    }

    return fromTextFile(configurationFile);
  }
}
