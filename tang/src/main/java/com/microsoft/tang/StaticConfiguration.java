package com.microsoft.tang;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;


final public class StaticConfiguration {
  public interface BindInterface { }
  public static final class Register implements BindInterface {
    final Class<?> clazz;
    public Register(Class<?> clazz) { this.clazz = clazz; }
  }
  public static final class Bind implements BindInterface {
    public Bind(Class<?> inter, Class<?> impl) { }
    public Bind(String inter, String impl) { }
  }
  public static final class BindImplementation implements BindInterface {
    public <T> BindImplementation(Class<T> inter, Class<? extends T> impl) { }
  }
  public static final class BindSingletonImplementation implements BindInterface {
    public <T> BindSingletonImplementation(Class<T> inter, Class<? extends T> impl) { }
  }
  public static final class BindSingleton implements BindInterface {
    public <T> BindSingleton(Class<T> singleton) { }
  }
  public static final class BindNamedParameter implements BindInterface {
    public <T> BindNamedParameter(Class<? extends Name<T>> name, String value) {}
  }
  public static final class BindConstructor implements BindInterface {
    public <T> BindConstructor(Class<T> inter, Class<? extends ExternalConstructor<T>> constructor) {}
  }
  class StaticConfigurationError extends RuntimeException {
    private static final long serialVersionUID = 1L;
    StaticConfigurationError(Throwable cause) { super(cause); }
  }
  public StaticConfiguration(BindInterface... b) throws StaticConfigurationError {
    try {
      ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      for(BindInterface bi : b) {
        if(bi instanceof Register) {
          cb.register(((Register) bi).clazz);
        }
      }
      cb.build();
    } catch (BindException e) {
      throw new StaticConfigurationError(e);
    }
  }
  public StaticConfiguration(Class<?>[][] c) {}
  
  static final StaticConfiguration config = new StaticConfiguration(
      new Register(String.class),
      new BindImplementation(Number.class, Integer.class)
  );
  static final StaticConfiguration config2 = new StaticConfiguration(
      new Class[][] {
          {String.class},
          {String.class,Integer.class}
      });
}
