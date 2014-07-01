package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.common.client.RunningJobImpl;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.reef.runtime.hdinsight.client.sslhacks.DefaultClientConstructor;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.wake.remote.RemoteConfiguration;

import javax.ws.rs.client.Client;

/**
 * The static part of the HDInsightRuntimeConfiguration.
 */
public final class HDInsightRuntimeConfigurationStatic extends ConfigurationModuleBuilder {

  public static final ConfigurationModule CONF = new HDInsightRuntimeConfigurationStatic()
      .bindImplementation(REEF.class, REEFImplementation.class)
      .bindImplementation(RunningJob.class, RunningJobImpl.class)
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindImplementation(JobSubmissionHandler.class, HDInsightJobSubmissionHandler.class)
      .bindConstructor(Client.class, DefaultClientConstructor.class)
      .build();

}
