package org.apache.reef.runime.azbatch.client;

import org.apache.reef.runtime.common.client.CommonRuntimeConfiguration;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.util.logging.LoggingSetup;

public class AzureBatchRuntimeConfigurationStatic extends ConfigurationModuleBuilder {
  static {
    LoggingSetup.setupCommonsLogging();
  }

  public static final ConfigurationModule CONF = new AzureBatchRuntimeConfigurationStatic()
      .merge(CommonRuntimeConfiguration.CONF)
      // Bind the Azure Batch runtime
      .bindImplementation(JobSubmissionHandler.class, AzureBatchJobSubmissionHandler.class)
      .bindImplementation(DriverConfigurationProvider.class, AzureBatchDriverConfigurationProviderImpl.class)
      .build();
}
