/*
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
package org.apache.reef.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Generic class which provides a simple mechanism to call a single method
 * on a concrete class.
 * @param <TReturn> The class type of the method return value.
 */
public final class MethodCallable<TReturn> implements Callable<TReturn> {
  private final Object obj;
  private final Object input;
  private final Method method;

  /**
   * @param obj An subclass of object on which the method will be invoked.
   * @param input Parameter input values for the method invocation.
   * @param function A string which contains the name of the method to be invoked.
   */
  public MethodCallable(final Object obj, final String function,
                        final Object... input) throws NoSuchMethodException {
    this.obj = obj;
    this.input = input;

    // Get the argument types.
    Class[] inputClass =  new Class[input.length];
    for (int idx = 0; idx < input.length; ++idx) {
      inputClass[idx] = input[idx].getClass();
    }

    this.method = obj.getClass().getDeclaredMethod(function, inputClass);
  }

  /**
   * @return A object of class type TReturn.
   * @throws InvocationTargetException The function specified in the constructor threw
   *         an exception.
   * @throws IllegalAccessException Method is not accessible in calling context.
   */
  @SuppressWarnings("unchecked")
  public TReturn call() throws InvocationTargetException, IllegalAccessException {
    return (TReturn)method.invoke(obj, input);
  }
}
