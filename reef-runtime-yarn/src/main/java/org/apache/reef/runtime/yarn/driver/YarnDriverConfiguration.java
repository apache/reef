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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.WorkingDirectoryTempFileCreator;
import org.apache.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchHandler;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseHandler;
import org.apache.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.time.Clock;

/**
 * Created by marku_000 on 2014-07-07.
 */
public class YarnDriverConfiguration extends ConfigurationModuleBuilder {
  /**
   * @see org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory
   */
  public static final RequiredParameter<String> JOB_SUBMISSION_DIRECTORY = new RequiredParameter<>();
  /**
   * @see org.apache.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod.class
   */
  public static final OptionalParameter<Integer> YARN_HEARTBEAT_INTERVAL = new OptionalParameter<>();

  /**
   * @see AbstractDriverRuntimeConfiguration.JobIdentifier.class
   */
  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see AbstractDriverRuntimeConfiguration.EvaluatorTimeout
   */
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();

  /**
   * The client remote identifier.
   */
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();

  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();


  public static ConfigurationModule CONF = new YarnDriverConfiguration()
      // Bind the YARN runtime for the resource manager.
      .bindImplementation(ResourceLaunchHandler.class, YARNResourceLaunchHandler.class)
      .bindImplementation(ResourceReleaseHandler.class, YARNResourceReleaseHandler.class)
      .bindImplementation(ResourceRequestHandler.class, YarnResourceRequestHandler.class)
      .bindConstructor(YarnConfiguration.class, YarnConfigurationConstructor.class)
      .bindSetEntry(Clock.RuntimeStartHandler.class, YARNRuntimeStartHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, YARNRuntimeStopHandler.class)
      .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)

          // Bind the YARN Configuration parameters
      .bindNamedParameter(JobSubmissionDirectory.class, JOB_SUBMISSION_DIRECTORY)
      .bindNamedParameter(YarnHeartbeatPeriod.class, YARN_HEARTBEAT_INTERVAL)

          // Bind the fields bound in AbstractDriverRuntimeConfiguration
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, JOB_IDENTIFIER)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
      .bindNamedParameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
      .build();
}
