/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang;

import java.net.URL;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.TangImpl;
/**
 * The root factory interface for Tang.  This interface allows callers to
 * instantiate Tang's core API, and is responsible for memoization and other
 * runtime optimizations. 
 */
public interface Tang {

  /**
   * Returns an Injector for the given Configurations.
   * 
   * @throws BindException
   *           If the confs conflict, a BindException will be thrown.
   */
  public Injector newInjector(final Configuration... confs)
      throws BindException;

  /**
   * Returns an Injector for the given Configuration.
   */
  public Injector newInjector(final Configuration confs);
  
  /**
   * Returns an Injector based on an empty Configuration.
   */
  public Injector newInjector();
  /**
   * Return a new ConfigurationBuilder that is backed by the provided
   * ClassHierarchy object.
   * @param ch Any valid Tang ClassHierarchy, including ones derived from non-Java application binaries.
   * @return an instance of ConfigurationBuilder.  In Tang's default implementation this returns an instance or JavaConfigurationBuilder if ch is backed by a Java classloader.
   */
  public ConfigurationBuilder newConfigurationBuilder(ClassHierarchy ch);

  /**
   * Create a new ConfigurationBuilder that is backed by the default
   * classloader and the provided jars.  Following standard Java's semantics,
   * when looking up a class, Tang will consult the default classloader first,
   * then the first classpath entry / jar passed to this method, then the
   * second, and so on. 
   * 
   * @return a new JavaConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars);

  /**
   * Merge a set of configurations into a new JavaConfiurationBuilder.  If
   * the configurations conflict, this method will throw a bind exception.
   * 
   * The underlying ClassHierarchies and parameter parsers of the
   * configurations will be checked for consistency.  The returned
   * configuration builder will be backed by a ClassHierachy that incorporates
   * the classpath and parsers from all of the provided Configurations.
   * 
   * @return a new ConfigurationBuilder
   * @throws BindException if any of the configurations contain duplicated or
   *    conflicting bindings, or if the backing ClassHierarchy objects conflict
   *    in some way.
   */
  public JavaConfigurationBuilder newConfigurationBuilder(Configuration... confs)
      throws BindException;
 
  /**
   * Create an empty JavaConfigurationBuilder that is capable of parsing
   * application-specific configuration values.  The returned
   * JavaConfigurationBuilder will be backed by the default classloader.   
   * 
   * @return a new ConfigurationBuilder
   */
  public JavaConfigurationBuilder newConfigurationBuilder(@SuppressWarnings("unchecked") Class<? extends ExternalConstructor<?>>... parameterParsers)
      throws BindException;

  /**
   * Create a new JavaConfiguration builder that has additional jars,
   * incorporates existing configuration data and / or can parse
   * application-specific types.
   * 
   * @see The documentation for the other newConfigurationBuilder methods in
   *      this class for detailed information about each of the parameters to
   *      this method.
   */
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parameterParsers) throws BindException;

  /**
   * Create a new empty ConfigurationBuilder that is backed by the default
   * classloader.
   */
  public JavaConfigurationBuilder newConfigurationBuilder();

  /**
   * A factory that returns the default implementation of the Tang interface.
   */
  public final class Factory {
    /**
     * Return an instance of the default implementation of Tang.
     * @return
     */
    public static Tang getTang() {
      return new TangImpl();
    }
  }
  /**
   * @return an instance of JavaClassHierarchy that is backed by the default
   *   Java classloader.  ClassHierarchy objects are immutable, so multiple
   *   invocations of this method may return the same instance.
   */
  public JavaClassHierarchy getDefaultClassHierarchy();
  /**
   * @return a custom instance of JavaClassHierarchy.  ClassHierarchy objects
   *   are immutable, so multiple invocations of this method may return the
   *   same instance.
   *   
   * @TODO Is the class hierarchy returned here backed by the default
   *   classloader or not?  If not, then callers will need to merge it with
   *   getDefaultClassHierarchy().  If so, we should add a new method like
   *   getNonDefaultClassHiearchy() that takes the same options as this one.
   */
  public JavaClassHierarchy getDefaultClassHierarchy(URL[] jars, Class<? extends ExternalConstructor<?>>[] parsers);


}
