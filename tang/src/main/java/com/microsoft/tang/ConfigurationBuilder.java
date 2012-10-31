package com.microsoft.tang;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
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
   * @param c
   */
  public void addConfiguration(final Configuration c) throws BindException;

  /**
   * Bind classes to each other, based on their full class names.
   * 
   * @param iface
   * @param impl
   * @throws ClassNotFoundException
   */
  public <T> void bind(String iface, String impl)
      throws ClassNotFoundException, BindException;

  /**
   * Bind named parameters, implementations or external constructors, depending
   * on the types of the classes passed in.
   * 
   * @param iface
   * @param impl
   */
  public <T> void bind(Class<T> iface, Class<?> impl) throws BindException;

  /**
   * Binds the Class impl as the implementation of the interface iface
   * 
   * @param <T>
   * @param iface
   * @param impl
   */
  public <T> void bindImplementation(Class<T> iface, Class<? extends T> impl)
      throws BindException;

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
  public <T> void bindSingletonImplementation(Class<T> iface,
      Class<? extends T> impl) throws BindException;

  /**
   * Same as bindSingletonImplementation, except that the singleton class is
   * bound to itself.
   * 
   * @param <T>
   * @param impl
   * @throws BindException
   */
  public <T> void bindSingleton(Class<T> iface) throws BindException;

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
  public <T> void bindNamedParameter(Class<? extends Name<T>> name, String value)
      throws BindException, InjectionException;

  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException;

  public Configuration build();

  /**
   * TODO move this somewhere else.
   * 
   * @param option
   * @param cb
   */
  public void addCommandLineOption(Option option, CommandLineCallback cb);

  /**
   * @param args
   * @throws IOException 
   * @throws NumberFormatException
   * @throws ParseException
   */
  public <T> void processCommandLine(String[] args) throws BindException, IOException, ParseException;

  public void processConfigFile(final File istream) throws IOException,
      BindException, ParseException;
}