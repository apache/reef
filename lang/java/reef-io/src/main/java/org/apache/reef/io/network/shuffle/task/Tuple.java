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
package org.apache.reef.io.network.shuffle.task;

/**
 *
 */
public final class Tuple<K, V> {

  private final K key;
  private final V value;

  public Tuple(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public int getKeyHash() {
    return key.hashCode();
  }

  public V getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "ShuffleTuple[ key : " + key + " , value : " + value + " ]";
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (! (obj instanceof Tuple)) {
      return false;
    }
    return key.equals(((Tuple) obj).getKey()) && value.equals(((Tuple) obj).getValue());
  }
}
