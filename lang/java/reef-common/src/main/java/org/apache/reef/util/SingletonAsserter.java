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

import org.apache.reef.io.Tuple;

import java.util.HashSet;
import java.util.Set;

/**
 * A helper class that can be used to ensure that objects are only instantiated once.
 */
public final class SingletonAsserter {

  private static final Set<Class> ALL_CLASSES = new HashSet<>();

  private static final Set<Class> SINGLETONS_GLOBAL = new HashSet<>();

  private static final Set<Tuple<String, Class>> SINGLETONS_SCOPED = new HashSet<>();

  /**
   * This class operates purely in static mode.
   */
  private SingletonAsserter() {
  }

  /**
   * Check if a given class is instantiated only once.
   * @param clazz Class to check.
   * @return True if the class was not instantiated before, false otherwise.
   */
  public static synchronized boolean assertSingleton(final Class clazz) {
    return SINGLETONS_GLOBAL.add(clazz) && ALL_CLASSES.add(clazz);
  }

  /**
   * Check if given class is singleton within a particular environment or scope.
   * @param scopeId Environment id, e.g. Driver name or REEF job id.
   * @param clazz Class that must have no more than 1 instance in that environment.
   * @return true if the instance is unique within that particular environment, false otherwise.
   */
  public static synchronized boolean assertSingleton(final String scopeId, final Class clazz) {
    ALL_CLASSES.add(clazz);
    return SINGLETONS_SCOPED.add(new Tuple<>(scopeId, clazz)) && !SINGLETONS_GLOBAL.contains(clazz);
  }
}
