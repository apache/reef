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
package org.apache.reef.examples.group.utils.math;


import org.apache.reef.io.Tuple;

/**
 * Represents an immutable vector.
 */
public interface ImmutableVector {
  /**
   * Access the value of the Vector at dimension i
   *
   * @param i index
   * @return the value at index i
   */
  double get(int i);

  /**
   * The size (dimensionality) of the Vector
   *
   * @return the size of the Vector.
   */
  int size();

  /**
   * Computes the inner product with another Vector.
   *
   * @param that
   * @return the inner product between two Vectors.
   */
  double dot(Vector that);

  /**
   * Computes the computeSum of all entries in the Vector.
   *
   * @return the computeSum of all entries in this Vector
   */
  double sum();

  /**
   * Computes the L2 norm of this Vector.
   *
   * @return the L2 norm of this Vector.
   */
  double norm2();

  /**
   * Computes the square of the L2 norm of this Vector.
   *
   * @return the square of the L2 norm of this Vector.
   */
  double norm2Sqr();

  /**
   * Computes the min of all entries in the Vector
   *
   * @return the min of all entries in this Vector
   */
  Tuple<Integer, Double> min();
}
