/**
 * Copyright (C) 2013 Microsoft Corporation
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
import com.microsoft.reef.runtime.common.REEFMessageCodec;
import com.microsoft.reef.runtime.common.client.ClientManager;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.yarn.util.YarnConfigurationConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.wake.remote.RemoteConfiguration;

/**
 * A ConfigurationModule for the YARN runtime.
 */
@Public
@ClientSide
public class YarnClientConfiguration extends ConfigurationModuleBuilder {
  // TODO: This has only one user and should be moved there.
  private final static String REEF_CLASSPATH = System.getenv("REEF_CLASSPATH");


  @NamedParameter(doc = "The job priority.", default_value = "0", short_name = "yarn_priority")
  public final static class JobPriority implements Name<Integer> {
  }

  @NamedParameter(doc = "The job queue.", default_value = "default", short_name = "yarn_queue")
  public final static class JobQueue implements Name<String> {
  }

  public static final OptionalParameter<String> YARN_QUEUE_NAME = new OptionalParameter<>();
  public static final OptionalParameter<Integer> YARN_PRIORITY = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new YarnClientConfiguration()
      // Bind the common runtime
      .bindImplementation(REEF.class, ClientManager.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
          // Bind the message codec for REEF.
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
          // Bind YARN
      .bindImplementation(JobSubmissionHandler.class, YarnJobSubmissionHandler.class)
          // Bind the parameters given by the user
      .bindNamedParameter(JobQueue.class, YARN_QUEUE_NAME)
      .bindNamedParameter(JobPriority.class, YARN_PRIORITY)
          // Bind external constructors. Taken from  YarnExternalConstructors.registerClientConstructors
      .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
      .build();
}
