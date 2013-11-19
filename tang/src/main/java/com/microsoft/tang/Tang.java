package com.microsoft.tang;

import java.net.URL;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.TangImpl;

public interface Tang {

  /**
   * Returns an Injector for the given Configurations.
   * 
   * @throws BindException
   *           If the confs conflict, a BindException will be thrown.
   */
  public Injector newInjector(final Configuration... confs)
      throws BindException;

  /**
   * Returns an Injector for the given Configuration.
   */
  public Injector newInjector(final Configuration confs);
  
  /**
   * Returns an Injector based on an empty Configuration.
   */
  public Injector newInjector();
  
  public ConfigurationBuilder newConfigurationBuilder(ClassHierarchy ch);

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars);

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(Configuration... confs)
      throws BindException;
 
  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(@SuppressWarnings("unchecked") Class<? extends ExternalConstructor<?>>... parameterParsers)
      throws BindException;

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parameterParsers) throws BindException;

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   * 
   */
  public JavaConfigurationBuilder newConfigurationBuilder();

  /**
   * Access to a ConfigurationBuilderImpl implementation
   */
  public final class Factory {
    public static Tang getTang() {
      return new TangImpl();
    }
  }
  public JavaClassHierarchy getDefaultClassHierarchy();
  public JavaClassHierarchy getDefaultClassHierarchy(URL[] jars, Class<? extends ExternalConstructor<?>>[] parsers);


}
