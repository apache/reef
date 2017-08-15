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
 * @param <TInput> The class type of the method input parameter.
 * @param <TClass> The class type of the class on which the method resides.
 */
public final class AsynchronousCaller<
      TReturn extends Object, TInput extends Object, TClass extends Object> implements Callable<TReturn> {
  private TClass obj;
  private TInput input;
  private Method method;

  /**
   * @param obj An instance of type TClass on which the method will be invoked..
   * @param input Parameter input value for the method invokcation.
   * @param function A string which contains the name of the method to be invoked.
   */
  public AsynchronousCaller(final TClass obj, final TInput input, final String function) throws NoSuchMethodException {
    this.obj = obj;
    this.input = input;
    Class[] args =  new Class[]{input.getClass()};
    this.method = obj.getClass().getDeclaredMethod(function, args);
  }

  /**
   * @return A object of class type TReturn.
   * @throws Exception Which occurred when the target function was called
   */
  public TReturn call() throws InvocationTargetException, IllegalAccessException {
    return (TReturn)method.invoke(obj, input);
  }

  private AsynchronousCaller() {}
}
