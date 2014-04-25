/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.canberra.math;

import java.io.Serializable;
import java.util.Random;

/**
 * A dense {@link Vector} implementation backed by a double[]
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public class DenseVector extends AbstractVector implements Serializable {

  private static final long serialVersionUID = 1L;
  private final double[] values;

  /**
   * Creates a dense vector of the given size
   *
   * @param size
   */
  public DenseVector(final int size) {
    this(new double[size]);

  }

  public DenseVector(final double[] values) {
    this.values = values;

  }

  @Override
  public void set(final int i, final double v) {
    this.values[i] = v;

  }

  @Override
  public double get(final int i) {
    return this.values[i];
  }

  @Override
  public int size() {
    return this.values.length;
  }

  /**
   * Access the underlying storage. This is unsafe.
   *
   * @return
   */
  public double[] getValues() {
    return this.values;
  }

  /**
   * Creates a random Vector of size 'size' where each element is individually
   * drawn from Math.random()
   *
   * @param size
   * @return a random Vector of the given size where each element is
   * individually drawn from Math.random()
   */
  public static DenseVector rand(final int size) {
    return rand(size, new Random());
  }

  /**
   * Creates a random Vector of size 'size' where each element is individually
   * drawn from Math.random()
   *
   * @param size
   * @param random the random number generator to use.
   * @return a random Vector of the given size where each element is
   * individually drawn from Math.random()
   */
  public static DenseVector rand(final int size, final Random random) {
    final DenseVector vec = new DenseVector(size);
    for (int i = 0; i < size; ++i) {
      vec.values[i] = random.nextDouble();
    }
    return vec;
  }

  /**
   * Instantiates a new DenseVector by copying the given other vector.
   *
   * @param other
   * @return
   */
  public static DenseVector copy(final ImmutableVector other) {
    final int size = other.size();
    final double[] values = new double[size];
    for (int i = 0; i < size; ++i) {
      values[i] = other.get(i);
    }
    return new DenseVector(values);
  }


}
