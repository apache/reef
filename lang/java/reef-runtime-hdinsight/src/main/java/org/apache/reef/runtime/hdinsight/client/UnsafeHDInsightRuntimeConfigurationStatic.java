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
package org.apache.reef.runtime.hdinsight.client;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.reef.client.REEF;
import org.apache.reef.client.RunningJob;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.REEFImplementation;
import org.apache.reef.runtime.common.client.RunningJobImpl;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.runtime.hdinsight.HDInsightClasspathProvider;
import org.apache.reef.runtime.hdinsight.client.sslhacks.UnsafeClientConstructor;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.util.logging.LoggingSetup;
import org.apache.reef.wake.remote.RemoteConfiguration;

/**
 * The static part of the UnsafeHDInsightRuntimeConfiguration.
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
      .bindImplementation(DriverConfigurationProvider.class, DriverConfigurationProviderImpl.class)
      .bindConstructor(CloseableHttpClient.class, UnsafeClientConstructor.class)
      .bindImplementation(RuntimeClasspathProvider.class, HDInsightClasspathProvider.class)
      .build();

}
