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
package org.apache.reef.runtime.standalone.client;

import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.local.client.LocalJobSubmissionHandler;
import org.apache.reef.runtime.local.client.ExecutorServiceConstructor;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.standalone.client.parameters.NodeFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeListFilePath;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.tang.ConfigurationProvider;
import org.apache.reef.tang.formats.*;

import java.util.concurrent.ExecutorService;

/**
 * A ConfigurationModule to configure the standalone resourcemanager.
 */
public final class StandaloneRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The file with information of remote nodes.
   */
  public static final RequiredParameter<String> NODE_LIST_FILE_PATH = new RequiredParameter<>();
  /**
   * Destination folder for the remote nodes.
   */
  public static final OptionalParameter<String> NODE_FOLDER = new OptionalParameter<>();

  /**
   * The folder in which the sub-folders, one per Node, will be created. Those will contain one folder per
   * Evaluator instantiated on the virtual node. Those inner folders will be named by the time when the Evaluator was
   * launched.
   * <p>
   * If none is given, a folder "REEF_STANDALONE_RUNTIME" will be created in the local directory.
   */
  public static final OptionalParameter<String> RUNTIME_ROOT_FOLDER = new OptionalParameter<>();

  /**
   * Configuration provides whose Configuration will be merged into all Driver Configuration.
   */
  public static final OptionalImpl<ConfigurationProvider> DRIVER_CONFIGURATION_PROVIDERS = new OptionalImpl<>();

  /**
   * The ConfigurationModule for the standalone resourcemanager.
   */
  public static final ConfigurationModule CONF = new StandaloneRuntimeConfiguration()
      .merge(CommonRuntimeConfiguration.CONF)
          // Bind the standalone runtime
      .bindImplementation(JobSubmissionHandler.class, LocalJobSubmissionHandler.class)
      .bindImplementation(DriverConfigurationProvider.class, StandaloneDriverConfigurationProviderImpl.class)
      .bindConstructor(ExecutorService.class, ExecutorServiceConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
          // Bind parameters of the standalone runtime
      .bindNamedParameter(NodeListFilePath.class, NODE_LIST_FILE_PATH)
      .bindNamedParameter(NodeFolder.class, NODE_FOLDER)
      .bindNamedParameter(RootFolder.class, RUNTIME_ROOT_FOLDER)
      .bindSetEntry(DriverConfigurationProviders.class, DRIVER_CONFIGURATION_PROVIDERS)
      .build();


}
