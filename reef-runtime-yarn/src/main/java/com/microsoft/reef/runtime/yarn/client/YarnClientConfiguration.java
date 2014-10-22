/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.yarn.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.files.RuntimeClasspathProvider;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.yarn.YarnClasspathProvider;
import com.microsoft.reef.runtime.yarn.client.parameters.JobPriority;
import com.microsoft.reef.runtime.yarn.client.parameters.JobQueue;
import com.microsoft.reef.runtime.yarn.util.YarnConfigurationConstructor;
import com.microsoft.reef.util.logging.LoggingSetup;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.wake.remote.RemoteConfiguration;

/**
 * A ConfigurationModule for the YARN resourcemanager.
 */
@Public
@ClientSide
public class YarnClientConfiguration extends ConfigurationModuleBuilder {
  static {
    LoggingSetup.setupCommonsLogging();
  }

  public static final OptionalParameter<String> YARN_QUEUE_NAME = new OptionalParameter<>();
  public static final OptionalParameter<Integer> YARN_PRIORITY = new OptionalParameter<>();

  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new YarnClientConfiguration()
      // Bind the common resourcemanager
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
          // Bind the message codec for REEF.
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          // Bind YARN
      .bindImplementation(JobSubmissionHandler.class, YarnJobSubmissionHandler.class)
          // Bind the parameters given by the user
      .bindNamedParameter(JobQueue.class, YARN_QUEUE_NAME)
      .bindNamedParameter(JobPriority.class, YARN_PRIORITY)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
          // Bind external constructors. Taken from  YarnExternalConstructors.registerClientConstructors
      .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
      .build();

}
