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
package org.apache.reef.io;

import org.apache.reef.annotations.Unstable;

import java.util.Map;
import java.util.Set;

/**
 * A Map interface for data that can be swapped out to other processes, managed
 * by native code, paged to disk, stored remotely, etc...
 *
 * @param <T> the entry type of the map
 */
@Unstable
public interface ExternalMap<T> {

  /**
   * @param key
   * @return true, if an entry with the given key exists
   */
  boolean containsKey(CharSequence key);

  /**
   * Element access.
   *
   * @param key
   * @return the object stored under key nor null if no such object exists
   */
  T get(CharSequence key);

  /**
   * Put a record into the map.
   *
   * @param key
   * @param value
   * @return the previous value associated with key, or null if there was no
   * mapping for key. (A null return can also indicate that the map previously
   * associated null with key, if the implementation supports null values.)
   */
  T put(CharSequence key, T value);

  /**
   * Removes the mapping for a key from this map if it is present (optional
   * operation). More formally, if this map contains a mapping from key k to
   * value v such that (key==null ? k==null : key.equals(k)), that mapping is
   * removed. (The map can contain at most one such mapping.) Returns the
   * value to which this map previously associated the key, or null if the map
   * contained no mapping for the key.
   * <p>
   * If this map permits null values, then a return value of null does not
   * necessarily indicate that the map contained no mapping for the key; it's
   * also possible that the map explicitly mapped the key to null.
   * <p>
   * The map will not contain a mapping for the specified key once the call
   * returns.
   *
   * @param key key whose mapping is to be removed from the map
   * @return the previous value associated with key, or null if there was no
   * mapping for key.
   */
  T remove(CharSequence key);

  /**
   * Copies all of the mappings from the specified map to this map (optional
   * operation). The effect of this call is equivalent to that of calling
   * put(k, v) on this map once for each mapping from key k to value v in the
   * specified map. The behavior of this operation is undefined if the
   * specified map is modified while the operation is in progress.
   *
   * @param m
   */
  void putAll(Map<? extends CharSequence, ? extends T> m);

  Iterable<Map.Entry<CharSequence, T>> getAll(Set<? extends CharSequence> keys);
}
