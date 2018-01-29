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
package org.apache.reef.runime.azbatch.driver;

import org.apache.reef.runime.azbatch.AzureBatchClasspathProvider;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.driver.parameters.DefinedRuntimes;
import org.apache.reef.runtime.common.driver.parameters.EvaluatorTimeout;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountContainerName;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountKey;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountName;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * ConfigurationModule to create YARN Driver configurations.
 */
public class AzureBatchDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * @see JobIdentifier
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see DefinedRuntimes
   */
  public static final RequiredParameter<String> RUNTIME_NAME = new RequiredParameter<>();

  /**
   * @see EvaluatorTimeout
   */
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();

  /**
   * The client remote identifier.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

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
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new AzureBatchDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, AzureBatchResourceLaunchHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, AzureBatchResourceReleaseHandler.class)
      .bindImplementation(ResourceRequestHandler.class, AzureBatchResourceRequestHandler.class)
      .bindImplementation(ResourceManagerStartHandler.class, AzureBatchResourceManagerStartHandler.class)
      .bindImplementation(ResourceManagerStopHandler.class, AzureBatchResourceManagerStopHandler.class)

      // Bind Azure Batch Configuration Parameters
      .bindNamedParameter(AzureBatchAccountUri.class, AZURE_BATCH_ACCOUNT_URI)
      .bindNamedParameter(AzureBatchAccountName.class, AZURE_BATCH_ACCOUNT_NAME)
      .bindNamedParameter(AzureBatchAccountKey.class, AZURE_BATCH_ACCOUNT_KEY)
      .bindNamedParameter(AzureBatchPoolId.class, AZURE_BATCH_POOL_ID)
      .bindNamedParameter(AzureStorageAccountName.class, AZURE_STORAGE_ACCOUNT_NAME)
      .bindNamedParameter(AzureStorageAccountKey.class, AZURE_STORAGE_ACCOUNT_KEY)
      .bindNamedParameter(AzureStorageAccountContainerName.class, AZURE_STORAGE_CONTAINER_NAME)

      // Bind the fields bound in AbstractDriverRuntimeConfiguration
      .bindNamedParameter(JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(LaunchID.class, JOB_IDENTIFIER)
      .bindNamedParameter(EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
      .bindNamedParameter(ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(ErrorHandlerRID.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, AzureBatchClasspathProvider.class)
      .bindSetEntry(DefinedRuntimes.class, RUNTIME_NAME)
      .build();
}
