package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.runtime.hdinsight.client.sslhacks.ClientProvider;
import com.microsoft.reef.runtime.hdinsight.client.sslhacks.TrustingClientProvider;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.wake.remote.RemoteConfiguration;

/**
 * The static part of the UnsafeHDInsightRuntimeConfiguration
 */
public final class UnsafeHDInsightRuntimeConfigurationStatic extends ConfigurationModuleBuilder {

  public static final ConfigurationModule CONF = new UnsafeHDInsightRuntimeConfigurationStatic()
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindImplementation(JobSubmissionHandler.class, HDInsightJobSubmissionHandler.class)
      .bindImplementation(ClientProvider.class, TrustingClientProvider.class)
      .build();

}
