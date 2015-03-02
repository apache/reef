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

import java.io.Serializable;

/**
 * An interface for Linear Alebra Vectors.
 */
public interface Vector extends ImmutableVector, Serializable {

  /**
   * Set dimension i of the Vector to value v
   *
   * @param i the index
   * @param v value
   */
  public void set(final int i, final double v);

  /**
   * Adds the Vector that to this one in place: this += that.
   *
   * @param that
   */
  public void add(final Vector that);

  /**
   * this += factor * that.
   *
   * @param factor
   * @param that
   */
  public void multAdd(final double factor, final ImmutableVector that);

  /**
   * Scales this Vector: this *= factor.
   *
   * @param factor the scaling factor.
   */
  public void scale(final double factor);


  /**
   * Normalizes the Vector according to the L2 norm.
   */
  public void normalize();

  /**
   * Create a new instance of the current type
   * with elements being zero
   *
   * @return zero vector of current dimensionality
   */
  public Vector newInstance();

}
