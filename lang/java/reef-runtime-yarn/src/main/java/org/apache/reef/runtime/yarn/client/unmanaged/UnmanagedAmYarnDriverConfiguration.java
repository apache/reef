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
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.parameters.DriverIsUnmanaged;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.io.WorkingDirectoryTempFileCreator;
import org.apache.reef.runtime.common.UserCredentials;
import org.apache.reef.runtime.common.driver.api.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.driver.parameters.DefinedRuntimes;
import org.apache.reef.runtime.common.driver.parameters.EvaluatorTimeout;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.REEFErrorHandler;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.driver.*;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.formats.*;
import org.apache.reef.wake.remote.RemoteConfiguration;

/**
 * Build configuration for REEF driver running in unmanaged mode under the YARN resource manager.
 */
@Public
@DriverSide
public final class UnmanagedAmYarnDriverConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> JOB_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> JOB_SUBMISSION_DIRECTORY = new RequiredParameter<>();

  public static final OptionalParameter<Integer> YARN_HEARTBEAT_INTERVAL = new OptionalParameter<>();
  public static final OptionalImpl<RackNameFormatter> RACK_NAME_FORMATTER = new OptionalImpl<>();
  public static final OptionalParameter<Long> EVALUATOR_TIMEOUT = new OptionalParameter<>();
  public static final OptionalParameter<String> CLIENT_REMOTE_IDENTIFIER = new OptionalParameter<>();
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  public static final ConfigurationModule CONF =
      new UnmanagedAmYarnDriverConfiguration()
          .bindNamedParameter(LaunchID.class, JOB_IDENTIFIER)
          .bindNamedParameter(JobIdentifier.class, JOB_IDENTIFIER)
          .bindNamedParameter(JobSubmissionDirectory.class, JOB_SUBMISSION_DIRECTORY)
          // REEF client parameters
          .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_UNMANAGED_DRIVER")
          .bindNamedParameter(RemoteConfiguration.ErrorHandler.class, REEFErrorHandler.class)
          .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          // YARN runtime
          .bindNamedParameter(DriverIsUnmanaged.class, "true")
          .bindImplementation(ResourceLaunchHandler.class, YARNResourceLaunchHandler.class)
          .bindImplementation(ResourceReleaseHandler.class, YARNResourceReleaseHandler.class)
          .bindImplementation(ResourceRequestHandler.class, YarnResourceRequestHandler.class)
          .bindImplementation(ResourceManagerStartHandler.class, YARNRuntimeStartHandler.class)
          .bindImplementation(ResourceManagerStopHandler.class, YARNRuntimeStopHandler.class)
          .bindConstructor(YarnConfiguration.class, YarnConfigurationConstructor.class)
          .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)
          .bindNamedParameter(YarnHeartbeatPeriod.class, YARN_HEARTBEAT_INTERVAL)
          // AbstractDriverRuntime parameters
          .bindNamedParameter(EvaluatorTimeout.class, EVALUATOR_TIMEOUT)
          .bindNamedParameter(ClientRemoteIdentifier.class, CLIENT_REMOTE_IDENTIFIER)
          .bindNamedParameter(ErrorHandlerRID.class, CLIENT_REMOTE_IDENTIFIER)
          .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
          .bindImplementation(RackNameFormatter.class, RACK_NAME_FORMATTER)
          .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
          .bindImplementation(UserCredentials.class, YarnProxyUser.class)
          .bindNamedParameter(DefinedRuntimes.class, RuntimeIdentifier.RUNTIME_NAME)
          .build();

  /** Cannot instantiate this utility class. */
  private UnmanagedAmYarnDriverConfiguration() { }
}
