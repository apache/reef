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

import java.util.Collection;

/**
 * Utility functions for arrays and collection classes.
 */
public final class CollectionUtils {

  private CollectionUtils() {
    // avoid instantiation
  }

  /**
   * Checks if the collection is null or empty.
   * @param <T> a type of element of collection
   * @param parameter the collection
   * @return true if the collection is null or empty
   */
  public static <T> boolean isEmpty(final Collection<T> parameter) {
    return parameter == null || parameter.isEmpty();
  }

  /**
   * Checks if the collection is not null and not empty.
   * @param <T> a type of element of collection
   * @param parameter the collection
   * @return true if the collection is not null nor empty
   */
  public static <T> boolean isNotEmpty(final Collection<T> parameter) {
    return !isEmpty(parameter);
  }

  private static final Object[] EMPTY_ARRAY = new Object[0];

  /**
   * Return an empty array if input is null; works as identity function otherwise.
   * This function is useful in `for` statements if the iterable can be null.
   * @param array Input array. Can be null.
   * @param <T> Type of the elements of the array.
   * @return A reference to the input array if it is not null, an empty array otherwise.
   */
  @SuppressWarnings("unchecked")
  public static <T> T[] nullToEmpty(final T[] array) {
    return array == null ? (T[])EMPTY_ARRAY : array;
  }
}
