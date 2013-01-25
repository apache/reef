package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;


final public class StaticConfiguration {
  public interface BindInterface {
    void configure(ConfigurationBuilder cb) throws BindException;
  }
  public static final class AddConfiguration implements BindInterface {
    final StaticConfiguration sc;
    public AddConfiguration(StaticConfiguration sc) {
      this.sc = sc;
    }
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.addConfiguration(sc.build());
    }
  }
  public static final class Register implements BindInterface {
    final Class<?> clazz;
    public Register(Class<?> clazz) { this.clazz = clazz; }
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.register(clazz);
    }
  }
  public static final class RegisterLegacyConstructor implements BindInterface {
    final Class<?> c;
    final Class<?>[] args;
    public RegisterLegacyConstructor(Class<?> c, Class<?>...args) {
      this.c = c;
      this.args = args;
    }
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.registerLegacyConstructor(c, args);
    }
    
  }
  public static final class Bind implements BindInterface {
    final Class<?> inter;
    final Class<?> impl;
    public Bind(Class<?> inter, Class<?> impl) {
      this.inter = inter;
      this.impl = impl;
    }
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.bind(inter,  impl);
    }
  }
  public static final class BindImplementation implements BindInterface {
    final Class<?> inter;
    final Class<?> impl;
    public <T> BindImplementation(Class<T> inter, Class<? extends T> impl) {
      this.inter = inter;
      this.impl = impl;
    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.bindImplementation((Class)inter,  (Class)impl);
    }
  }
  public static final class BindSingletonImplementation implements BindInterface {
    final Class<?> inter;
    final Class<?> impl;
    public <T> BindSingletonImplementation(Class<T> inter, Class<? extends T> impl) {
      this.inter = inter;
      this.impl = impl;
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.bindSingletonImplementation((Class)inter, (Class)impl);
    }
  }
  public static final class BindSingleton implements BindInterface {
    final Class<?> singleton;
    public <T> BindSingleton(Class<T> singleton) {
      this.singleton = singleton;
    }
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.bindSingleton(singleton);
    }
  }
  public static final class BindNamedParameter implements BindInterface {
    final Class<? extends Name<?>> name;
    final String value;
    final Class<?> impl;
    public <T> BindNamedParameter(Class<? extends Name<T>> name, String value) {
      this.name = name;
      this.value = value;
      this.impl = null;
    }
    public <T> BindNamedParameter(Class<? extends Name<T>> name, Class<? extends T> impl) {
      this.name = name;
      this.impl = impl;
      this.value = null;
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      if(value != null) {
        cb.bindNamedParameter((Class)name,  value);
      } else {
        cb.bindNamedParameter((Class)name, (Class)impl);
      }
    }
  }
  public static final class BindConstructor implements BindInterface {
    final Class<?> inter;
    final Class<? extends ExternalConstructor<?>> constructor;
    public <T> BindConstructor(Class<T> inter, Class<? extends ExternalConstructor<T>> constructor) {
      this.inter = inter;
      this.constructor = constructor;
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void configure(ConfigurationBuilder cb) throws BindException {
      cb.bindConstructor((Class)inter, (Class)constructor);
    }
  }
  private final BindInterface[] b;
  private Configuration memo;
  
  public StaticConfiguration(BindInterface... b) {
    this.b = b;
  }
  
  public Configuration build() throws BindException {
    if(memo == null) {
      ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      for(BindInterface bi : b) {
        bi.configure(cb);
      }
      memo = cb.build();
    }
    return memo;
  }
}
