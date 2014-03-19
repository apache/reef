package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.test.RoundTripTest;

import java.io.File;

/**
 * Tests the file writing routines in ConfigurationFile.
 */
public final class ConfigurationFileTest extends RoundTripTest {
  @Override
  public Configuration roundTrip(final Configuration configuration) throws Exception {
    final File tempFile = java.io.File.createTempFile("TangTest", "txt");

    // Write the configuration.
    ConfigurationFile.writeConfigurationFile(configuration, tempFile);

    // Read it again.
    final JavaConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    ConfigurationFile.addConfiguration(configurationBuilder, tempFile);
    return configurationBuilder.build();
  }
}
