package com.microsoft.reef.io.data.loading.api;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.wake.time.Clock;

public final class DataLoadingDriverConfiguration extends ConfigurationModuleBuilder {

  public static final ConfigurationModule CONF = new DataLoadingDriverConfiguration().merge(DriverConfiguration.CONF)
      .bindSetEntry(Clock.StartHandler.class, DataLoader.StartHandler.class)
      .bindSetEntry(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class, DataLoader.EvaluatorAllocatedHandler.class)
      .build();

}
