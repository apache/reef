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
package org.apache.reef.runtime.multi.driver;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.driver.parameters.DefinedRuntimes;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.multi.client.parameters.SerializedRuntimeDefinition;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * ConfigurationModule for the multi-runtime Driver.
 */
@Private
@RuntimeAuthor
public class MultiRuntimeDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * Serialized runtime configuration.
   */
  public static final RequiredParameter<String> SERIALIZED_RUNTIME_DEFINITION = new RequiredParameter<>();

  /**
   * Serialized runtime configuration.
   */
  public static final RequiredParameter<String> RUNTIME_NAMES = new RequiredParameter<>();

  /**
   * The identifier of the Job submitted.
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * The identifier of the Job submitted.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * Hybrid driver configuration.
   */
  public static final ConfigurationModule CONF = new MultiRuntimeDriverConfiguration()
      .bindImplementation(ResourceLaunchHandler.class, MultiRuntimeResourceLaunchHandler.class)
      .bindImplementation(ResourceRequestHandler.class, MultiRuntimeResourceRequestHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, MultiRuntimeResourceReleaseHandler.class)
      .bindImplementation(ResourceManagerStartHandler.class, MultiRuntimeResourceManagerStartHandler.class)
      .bindImplementation(ResourceManagerStopHandler.class, MultiRuntimeResourceManagerStopHandler.class)
      .bindNamedParameter(SerializedRuntimeDefinition.class, SERIALIZED_RUNTIME_DEFINITION)
      .bindNamedParameter(LaunchID.class, JOB_IDENTIFIER)
      .bindNamedParameter(JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindSetEntry(DefinedRuntimes.class, RUNTIME_NAMES)
      .build();
}
