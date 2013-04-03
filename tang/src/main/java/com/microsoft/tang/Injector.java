package com.microsoft.tang;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;

public interface Injector {

  /**
   * Gets an instance of iface, or the implementation that has been bound to it.
   * Unless iface (or its implementation) has been registered as a singleton (by
   * ConfigurationBuilder.bindSingleton()) this method will return a new
   * instance each time it is called.
   * 
   * @param iface
   * @return
   * @throws NameResolutionException
   * @throws ReflectiveOperationException
   */
  public <U> U getInstance(Class<U> iface) throws InjectionException;

  public <U> U getInstance(String iface) throws InjectionException, NameResolutionException;

  public <U> U getNamedInstance(Class<? extends Name<U>> iface)
      throws InjectionException;

  /**
   * Gets the value stored for the given named parameter.
   * 
   * @param <U>
   * @param name
   * @return an Instance of the class configured as the implementation for the
   *         given interface class.
   * @throws InjectionException
   */
  public <T> T getNamedParameter(Class<? extends Name<T>> name)
      throws InjectionException;

  /**
   * Binds the given object to the class. Note that this only affects objects
   * created by the returned Injector and its children. Also, like all
   * Injectors, none of those objects can be serialized back to a configuration
   * file).
   * 
   * @param iface
   * @param inst
   * @return A copy of this injector that reflects the new binding.
   * @throws BindException
   */
  public <T> void bindVolatileInstance(Class<T> iface, T inst)
      throws BindException;

  public <T> void bindVolatileParameter(Class<? extends Name<T>> iface, T inst)
      throws BindException;

  /**
   * Create a new child Injector that inherits the singleton instances created
   * by this Injector, but reflects additional Configuration objects. This can
   * be used to create trees of Injectors that obey hierarchical scoping rules.
   * 
   * Except for the fact that the child Injector will have references to this
   * injector's singletons, the returned Injector is equivalent to the one you
   * would get by using ConfigurationBuilder to build a merged Configuration,
   * and then using the merged Configuration to create an Injector. Injectors
   * returned by ConfigurationBuilders are always independent, and so never
   * share references to the same singleton instances.
   * 
   * @throws BindException
   *           If any of the configurations conflict with each other, or the
   *           existing Injector's Configuration.
   */
  public Injector createChildInjector(Configuration... configurations)
      throws BindException;

  /**
   * Returns true if this Injector is able to instantiate the object named by
   * name.
   * 
   * @param name
   * @return
   * @throws BindException
   */
  boolean isInjectable(String name) throws BindException;

  boolean isParameterSet(String name) throws BindException;

  boolean isInjectable(Class<?> clazz) throws BindException;

  boolean isParameterSet(Class<? extends Name<?>> name) throws BindException;
}