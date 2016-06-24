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
package org.apache.reef.runtime.standalone.driver;

import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.LocalClasspathProvider;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeListFilePath;
import org.apache.reef.runtime.standalone.client.parameters.NodeInfoSet;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * ConfigurationModule for the Driver executed in the standalone resourcemanager. This is meant to eventually replace
 * StandaloneDriverRuntimeConfiguration.
 */
public class StandaloneDriverConfiguration extends ConfigurationModuleBuilder {
  /**
   * The root folder of the job. Assumed to be an absolute path.
   */
  public static final RequiredParameter<String> ROOT_FOLDER = new RequiredParameter<>();
  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  /**
   * The remote identifier to use for communications back to the client.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * The identifier of the Job submitted.
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * Set of nodes.
   */
  public static final RequiredParameter<String> NODE_LIST_FILE_PATH = new RequiredParameter<>();
  public static final RequiredParameter<String> NODE_INFO_SET = new RequiredParameter<>();

  /**
   * Folder where the file with the set of nodes are saved in.
   */
  public static final OptionalParameter<String> NODE_FOLDER = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new StandaloneDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, StandaloneResourceLaunchHandler.class)
      .bindImplementation(ResourceRequestHandler.class, StandaloneResourceRequestHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, StandaloneResourceReleaseHandler.class)
      .bindImplementation(ResourceManagerStartHandler.class, StandaloneResourceManagerStartHandler.class)
      .bindImplementation(ResourceManagerStopHandler.class, StandaloneResourceManagerStopHandler.class)
      .bindNamedParameter(ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(ErrorHandlerRID.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(LaunchID.class, JOB_IDENTIFIER)
      .bindNamedParameter(RootFolder.class, ROOT_FOLDER)
      .bindNamedParameter(NodeListFilePath.class, NODE_LIST_FILE_PATH)
      .bindNamedParameter(NodeFolder.class, NODE_FOLDER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindSetEntry(NodeInfoSet.class, NODE_INFO_SET)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
      .build();
}
