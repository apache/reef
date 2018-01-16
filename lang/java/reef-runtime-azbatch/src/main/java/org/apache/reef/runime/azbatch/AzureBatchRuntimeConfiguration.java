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
package org.apache.reef.runime.azbatch;

import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runime.azbatch.client.AzureBatchDriverConfigurationProvider;
import org.apache.reef.runime.azbatch.client.AzureBatchJobSubmissionHandler;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Configuration Module for the Azure Batch runtime.
 */
@Public
public class AzureBatchRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_URI = new RequiredParameter<>();

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> AZURE_BATCH_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> AZURE_BATCH_POOL_ID = new RequiredParameter<>();

  /**
   * The ConfigurationModule for the local resourcemanager.
   */
  public static final ConfigurationModule CONF = new AzureBatchRuntimeConfiguration()
      .merge(CommonRuntimeConfiguration.CONF)
      .bindNamedParameter(AzureBatchAccountUri.class, AZURE_BATCH_ACCOUNT_URI)
      .bindNamedParameter(AzureBatchAccountName.class, AZURE_BATCH_ACCOUNT_NAME)
      .bindNamedParameter(AzureBatchAccountKey.class, AZURE_BATCH_ACCOUNT_KEY)
      .bindNamedParameter(AzureBatchPoolId.class, AZURE_BATCH_POOL_ID)
      // Bind the Azure Batch runtime
      .bindImplementation(JobSubmissionHandler.class, AzureBatchJobSubmissionHandler.class)
      .bindImplementation(DriverConfigurationProvider.class, AzureBatchDriverConfigurationProvider.class)
      .build();
}
