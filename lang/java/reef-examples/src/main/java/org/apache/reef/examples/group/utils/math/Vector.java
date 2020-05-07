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
package org.apache.reef.examples.group.utils.math;

import java.io.Serializable;

/**
 * An interface for Linear Alebra Vectors.
 */
public interface Vector extends ImmutableVector, Serializable {

  /**
   * Set dimension i of the Vector to value v.
   *
   * @param i the index
   * @param v value
   */
  void set(int i, double v);

  /**
   * Adds the Vector that to this one in place: this += that.
   *
   * @param that
   */
  void add(Vector that);

  /**
   * this += factor * that.
   *
   * @param factor
   * @param that
   */
  void multAdd(double factor, ImmutableVector that);

  /**
   * Scales this Vector: this *= factor.
   *
   * @param factor the scaling factor.
   */
  void scale(double factor);


  /**
   * Normalizes the Vector according to the L2 norm.
   */
  void normalize();

  /**
   * Create a new instance of the current type.
   * with elements being zero
   *
   * @return zero vector of current dimensionality
   */
  Vector newInstance();

}
