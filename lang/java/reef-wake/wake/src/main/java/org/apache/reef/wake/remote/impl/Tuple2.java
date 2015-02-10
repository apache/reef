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
package org.apache.reef.wake.remote.impl;

/**
 * Tuple with two values
 *
 * @param <T1>
 * @param <T2>
 */
public class Tuple2<T1, T2> {

  private final T1 t1;
  private final T2 t2;

  public Tuple2(T1 t1, T2 t2) {
    this.t1 = t1;
    this.t2 = t2;
  }

  public T1 getT1() {
    return t1;
  }

  public T2 getT2() {
    return t2;
  }

  @Override
  public int hashCode() {
    return t1.hashCode() + 31 * t2.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    Tuple2<T1, T2> tuple = (Tuple2<T1, T2>) o;
    return t1.equals((Object) tuple.getT1()) && t2.equals((Object) tuple.getT2());
  }

  public String toString() {
    return t1.toString() + " " + t2.toString();
  }
}