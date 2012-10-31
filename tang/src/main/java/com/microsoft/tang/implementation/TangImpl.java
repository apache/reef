package com.microsoft.tang.implementation;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;

public class TangImpl implements Tang {

  @Override
  public Injector newInjector(Configuration... confs) {
    return new InjectorImpl(new ConfigurationBuilderImpl(confs).build());
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(ClassLoader... loaders) {
    if (loaders.length != 0) {
      throw new UnsupportedOperationException("loaders not implemented");
    }
    return new ConfigurationBuilderImpl();
  }

}
