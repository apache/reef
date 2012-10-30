package com.microsoft.tang;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.ConfigurationImpl;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl.CommandLineCallback;
import com.microsoft.tang.ExternalConstructor;

/**
 * A builder for TANG configurations.
 * 
 * @author sears
 * 
 */
public interface ConfigurationBuilder {
  /**
   * Add all configuration parameters from the given Configuration object.
   * 
   * @param conf
   */
  public void addConfiguration(final ConfigurationImpl conf);

  /**
   * Bind classes to each other, based on their full class names.
   * 
   * @param iface
   * @param impl
   * @throws ClassNotFoundException
   */
  public <T> void bind(String iface, String impl) throws ClassNotFoundException;

  /**
   * Bind named parameters, implementations or external constructors, depending
   * on the types of the classes passed in.
   * 
   * @param iface
   * @param impl
   */
  public <T> void bind(Class<T> iface, Class<?> impl);

  /**
   * Binds the Class impl as the implementation of the interface iface
   * 
   * @param <T>
   * @param iface
   * @param impl
   */
  public <T> void bindImplementation(Class<T> iface, Class<? extends T> impl);

  /**
   * Bind iface to impl, ensuring that all injections of iface return the same
   * instance of impl. Note that directly injecting an impl (and injecting
   * classes not bound "through" iface") will still create additional impl
   * instances.
   * 
   * If you want to ensure that impl is a singleton instead, use the single
   * argument version.
   * 
   * @param <T>
   * @param iface
   * @param impl
   * @throws BindException
   */
  public abstract <T> void bindSingletonImplementation(Class<T> iface,
      Class<? extends T> impl) throws ReflectiveOperationException;

  /**
   * Same as bindSingletonImplementation, except that the singleton class is
   * bound to itself.
   * 
   * @param <T>
   * @param impl
   * @throws BindException
   */
  public abstract <T> void bindSingleton(Class<T> iface)
      throws ReflectiveOperationException;

  /**
   * Set the value of a named parameter.
   * 
   * @param name
   *          The dummy class that serves as the name of this parameter.
   * @param value
   *          A string representing the value of the parameter. Reef must know
   *          how to parse the parameter's type.
   * @throws NameResolutionException
   */
  public abstract <T> void bindNamedParameter(Class<? extends Name<T>> name,
      String value);

  public abstract <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v);

  public abstract Configuration build();

  /**
   * TODO move this somewhere else.
   * 
   * @param option
   * @param cb
   */
  public void addCommandLineOption(Option option, CommandLineCallback cb);

  /**
   * TODO move this somewhere else.
   * 
   * @param args
   * @throws NumberFormatException
   * @throws ParseException
   */
  public <T> void processCommandLine(String[] args)
      throws NumberFormatException, ParseException;

}