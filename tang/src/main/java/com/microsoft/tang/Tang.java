package com.microsoft.tang;

import java.io.IOException;
import java.io.InputStream;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.TangImpl;

public interface Tang {

  /**
   * Returns an Injector for the given Configurations.
   * 
   * @param confs
   * @return
   * @throws BindException
   */
  public Injector newInjector(final Configuration... confs);

  /**
   * Reads a configuration from an InputStream
   * 
   * @param istream
   * @return
   * @throws IOException
   * @deprecated
   */
  public Configuration configurationFromStream(final InputStream istream)
      throws IOException;

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public ConfigurationBuilder newConfigurationBuilder(ClassLoader... loader)
      throws BindException;

  /**
   * Access to a ConfigurationBuilderImpl implementation
   */
  public final class Factory {
    public static Tang getTang() {
      return new TangImpl();
    }
  }
}
