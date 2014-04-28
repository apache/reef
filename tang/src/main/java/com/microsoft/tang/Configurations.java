package com.microsoft.tang;

/**
 * Helper class for Configurations
 */
public final class Configurations {

  /**
   * This is a utility class that isn't meant to be instantiated.
   */
  private Configurations() {
  }


  /**
   * Merge a set of Configurations.
   *
   * @param configurations
   * @return the merged configuration.
   * @throws com.microsoft.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Configuration... configurations) {
    return Tang.Factory.getTang().newConfigurationBuilder(configurations).build();
  }

  /**
   * Merge a set of Configurations.
   *
   * @param configurations
   * @return the merged configuration.
   * @throws com.microsoft.tang.exceptions.BindException if the merge fails.
   */
  public static Configuration merge(final Iterable<Configuration> configurations) {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Configuration configuration : configurations) {
      configurationBuilder.addConfiguration(configuration);
    }
    return configurationBuilder.build();
  }

}
