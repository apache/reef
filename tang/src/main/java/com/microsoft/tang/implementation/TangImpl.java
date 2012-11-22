package com.microsoft.tang.implementation;

import java.net.URL;

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
  public ConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }
  @Override
  public ConfigurationBuilder newConfigurationBuilder(URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(Configuration... confs)
      throws BindException {
    return newConfigurationBuilder(new URL[0], confs);

  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs) throws BindException {
    ConfigurationBuilderImpl cb = new ConfigurationBuilderImpl(jars);
    for (Configuration c : confs) {
      cb.addConfiguration(c);
    }
    return cb;
  }

}
