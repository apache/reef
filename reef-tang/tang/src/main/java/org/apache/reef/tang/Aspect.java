/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang;

import org.apache.reef.tang.types.ConstructorDef;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A simple interface that allows external code to interpose on Tang object
 * injections.  This can be used to implement simplistic aspect oriented
 * design patterns by interposing wrapper objects at injection time.  It
 * can also be used for more mundane purposes, such as tracking the
 * relationship between the objects that are instantiated at runtime.
 * <p/>
 * The Wake project contains a full-featured implementation of this API that
 * may serve as a useful example.
 */
public interface Aspect {
  /**
   * Inject an object of type T.
   * <p/>
   * Note that it is never OK to return an instance of ExternalConstructor.
   * Typical implementations check to see if they are about to return an
   * instance of ExternalConstructor.  If so, they return ret.newInstance()
   * instead.
   *
   * @param def         information about the constructor to be invoked.  This is
   *                    mostly useful because it contains references to any relevant named
   *                    parameters, and to the class to be injected.
   * @param constructor The java constructor to be injected.  Tang automatically
   *                    chooses the appropriate constructor and ensures that we have permission
   *                    to invoke it.
   * @param args        The parameters to be passed into constructor.newInstance(), in the correct order.
   * @return A new instance of T.
   * Note, it is inject()'s responsibility to call <tt>ret.getInstance() if ret instanceof ExternalConstructor</tt>.
   * @throws A number of exceptions which are passed-through from the wrapped call to newInstance().
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
