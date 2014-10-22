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
package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.runtime.hdinsight.client.sslhacks.UnsafeClientConstructor;
import com.microsoft.reef.util.logging.LoggingSetup;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.wake.remote.RemoteConfiguration;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * The static part of the UnsafeHDInsightRuntimeConfiguration
 */
public final class UnsafeHDInsightRuntimeConfigurationStatic extends ConfigurationModuleBuilder {
  static {
    LoggingSetup.setupCommonsLogging();
  }

  public static final ConfigurationModule CONF = new UnsafeHDInsightRuntimeConfigurationStatic()
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindImplementation(JobSubmissionHandler.class, HDInsightJobSubmissionHandler.class)
      .bindConstructor(CloseableHttpClient.class, UnsafeClientConstructor.class)
      .build();

}
