package com.microsoft.tang;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.microsoft.tang.types.ConstructorDef;

public interface Aspect {
  /**
   * Note, it is inject()'s responsibility to call ret.getInstance() if ret instanceof ExternalConstructor.
   */
  <T> T inject(ConstructorDef<T> def, Constructor<T> constructor, Object[] args) throws InvocationTargetException, IllegalAccessException, IllegalArgumentException, InstantiationException;
  /**
   * TANG calls this the first time get() is called on an injection future.  This informs the aspect of 
   * the relationship between InjectionFutures (that were already passed into inject()) and the instantiated
   * object.
   * 
   * @param f An InjectionFuture that was passed to the args[] array of inject at some point in the past. 
   * @param t An object instance that was returned by inject().
   */
  <T> void injectionFutureInstantiated(InjectionFuture<T> f, T t);
  /**
   * This method creates a child aspect, and returns it.  This allows aspects to track information about
   * Tang injection scopes.  If such information is not needed, it is legal for Aspect implementations to
   * return "this".
   */
  Aspect createChildAspect();
}
