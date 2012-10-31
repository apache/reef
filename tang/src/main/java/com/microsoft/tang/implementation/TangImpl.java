package com.microsoft.tang.implementation;

import java.io.IOException;
import java.io.InputStream;

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
  public Configuration configurationFromStream(InputStream istream)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(ClassLoader... loaders) {
    if (loaders.length != 0) {
      throw new UnsupportedOperationException("loaders not implemented");
    }
    return new ConfigurationBuilderImpl();
  }

}
