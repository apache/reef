package com.microsoft.tang;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.implementation.TangImpl;

public interface Tang {

  /**
   * Returns an Injector for the given Configurations.
   * 
   * This call eagerly binds singleton classes to instances.
   * 
   * @param confs
   * @return
   * @throws BindException If the confs conflict, a BindException will be thrown.
   * @throws InjectionException If any singletons fail to inject.
   */
  public Injector newInjector(final Configuration... confs) throws BindException, InjectionException;

  /**
   * Create a new ConfigurationBuilder
   * 
   * @return a new ConfigurationBuilder
   */
  public ConfigurationBuilder newConfigurationBuilder(ClassLoader... loader);

  /**
   * Access to a ConfigurationBuilderImpl implementation
   */
  public final class Factory {
    public static Tang getTang() {
      return new TangImpl();
    }
  }
}
