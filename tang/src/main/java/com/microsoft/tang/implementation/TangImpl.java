package com.microsoft.tang.implementation;

import java.net.URL;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.java.InjectorImpl;
import com.microsoft.tang.implementation.java.JavaConfigurationBuilderImpl;

public class TangImpl implements Tang {

  @Override
  public Injector newInjector(Configuration... confs) throws BindException {
    return new InjectorImpl(new JavaConfigurationBuilderImpl(confs).build());
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(Configuration... confs)
      throws BindException {
    return newConfigurationBuilder(new URL[0], confs);

  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs) throws BindException {
    JavaConfigurationBuilder cb = new JavaConfigurationBuilderImpl(jars);
    for (Configuration c : confs) {
      cb.addConfiguration(c);
    }
    return cb;
  }

}
