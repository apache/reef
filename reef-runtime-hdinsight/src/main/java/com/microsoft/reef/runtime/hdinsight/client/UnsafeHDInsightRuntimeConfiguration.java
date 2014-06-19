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
import com.microsoft.reef.runtime.hdinsight.client.sslhacks.TrustingClientConstructor;
import com.microsoft.reef.runtime.hdinsight.parameters.*;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.remote.RemoteConfiguration;

import javax.ws.rs.client.Client;

/**
 * Same as HDInsightRuntimeConfiguration, but ignores SSL errors on submission.
 */
public final class UnsafeHDInsightRuntimeConfiguration extends ConfigurationModuleBuilder {

  /**
   * The URL of the hdinsight web service. E.g. http://services.mycompany.com:1234/templeton/v1/
   */
  public static final RequiredParameter<String> URL = new RequiredParameter<>();

  /**
   * The Storage account to be used by Azure.
   */
  public static final RequiredParameter<String> STORAGE_ACCOUNT_NAME = new RequiredParameter<>();

  /**
   * The Storage account key to be used by Azure.
   */
  public static final RequiredParameter<String> STORAGE_ACCOUNT_KEY = new RequiredParameter<>();

  /**
   * The Container name to be used by Azure.
   */
  public static final RequiredParameter<String> CONTAINER_NAME = new RequiredParameter<>();

  /**
   * The username to be used for connecting to hdinsight.
   */
  public static final RequiredParameter<String> USER_NAME = new RequiredParameter<>();

  /**
   * The password to be used for connecting to hdinsight.
   */
  public static final RequiredParameter<String> PASSWORD = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new UnsafeHDInsightRuntimeConfiguration()
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)

          // Make WS API injectable
      .bindConstructor(Client.class, TrustingClientConstructor.class)

          // Uploader configuration
      .bindNamedParameter(AzureStorageAccountName.class, STORAGE_ACCOUNT_NAME)
      .bindNamedParameter(AzureStorageAccountKey.class, STORAGE_ACCOUNT_KEY)
      .bindNamedParameter(AzureStorageAccountContainerName.class, CONTAINER_NAME)

          // WebHCat configuration
      .bindImplementation(JobSubmissionHandler.class, HDInsightJobSubmissionHandler.class)
      .bindNamedParameter(HDInsightInstanceURL.class, URL)
      .bindNamedParameter(HDInsightUsername.class, USER_NAME)
      .bindNamedParameter(HDInsightPassword.class, PASSWORD)
      .build();
}
