package com.microsoft.tang;

import com.microsoft.tang.annotations.Name;
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
  public abstract <U> U getInstance(Class<U> iface)
      throws NameResolutionException, ReflectiveOperationException;

  /**
   * Gets the value stored for the given named parameter.
   * 
   * @param <U>
   * @param name
   * @return an Instance of the class configured as the implementation for the
   *         given interface class.
   * @throws InjectionException
   */
  public abstract <T> T getNamedParameter(Class<? extends Name<T>> name)
      throws ReflectiveOperationException, NameResolutionException;

  /**
   * Binds the given object to the class. Note that this only affects objects
   * created by this Injector (which cannot be serialized back to a
   * configuration file).
   * 
   * @param iface
   * @param inst
   * @throws NameResolutionException
   */
  public abstract <T> void bindVolatialeInstance(Class<T> iface, T inst);

}