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
package org.apache.reef.runtime.azbatch.client;

import org.apache.reef.annotations.audience.Public;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

import java.io.File;
import java.io.IOException;

/**
 * Configuration Module for the Azure Batch runtime.
 */
@Public
public final class AzureBatchRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The Azure Batch account URI.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_URI = new RequiredParameter<>();

  /**
   * The Azure Batch account name.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Azure Batch account key.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Azure Batch pool ID.
   */
  public static final RequiredParameter<String> AZURE_BATCH_POOL_ID = new RequiredParameter<>();

  /**
   * The environment variable that holds the path to the default configuration file.
   */
  public static final String AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE = "REEF_AZBATCH_CONF";

  /**
   * The Azure Storage account name.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Azure Storage account key.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Azure Storage container name.
   */
  public static final RequiredParameter<String> AZURE_STORAGE_CONTAINER_NAME = new RequiredParameter<>();

  /**
   * Container Registry Server.
   */
  public static final OptionalParameter<String> CONTAINER_REGISTRY_SERVER = new OptionalParameter<>();

  /**
   * Container Registry Username.
   */
  public static final OptionalParameter<String> CONTAINER_REGISTRY_USERNAME = new OptionalParameter<>();

  /**
   * Container Registry password.
   */
  public static final OptionalParameter<String> CONTAINER_REGISTRY_PASSWORD = new OptionalParameter<>();

  /**
   * Create a {@link Configuration} object from an Avro configuration file.
   *
   * @param file the configuration file.
   * @return the configuration object.
   * @throws IOException if the file can't be read
   */
  public static Configuration fromTextFile(final File file) throws IOException {
    return new AvroConfigurationSerializer().fromTextFile(file);
  }

  /**
   * Create a {@link Configuration} object from the
   * {@link AZBATCH_CONFIGURATION_FILE_ENVIRONMENT_VARIABLE} environment variable.
   *
   * @return the configuration object.
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
