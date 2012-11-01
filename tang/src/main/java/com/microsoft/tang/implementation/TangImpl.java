package com.microsoft.tang.implementation;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;

public class TangImpl implements Tang {

  @Override
  public Injector newInjector(Configuration... confs) throws BindException {
    return new InjectorImpl(new ConfigurationBuilderImpl(confs).build());
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(ClassLoader... loaders) {
    return new ConfigurationBuilderImpl(loaders);
  }

}
