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


import org.apache.reef.io.Tuple;

import java.util.Formatter;
import java.util.Locale;

/**
 * Base class for implementing ImmutableVector.
 */
abstract class AbstractImmutableVector implements ImmutableVector {

  @Override
  public abstract double get(int i);

  @Override
  public abstract int size();

  @Override
  public double dot(final Vector that) {
    assert this.size() == that.size();

    double result = 0.0;
    for (int index = 0; index < this.size(); ++index) {
      result += this.get(index) * that.get(index);
    }
    return result;
  }


  @Override
  public double sum() {
    double result = 0.0;
    for (int i = 0; i < this.size(); ++i) {
      result += this.get(i);
    }
    return result;
  }

  @Override
  public double norm2() {
    return Math.sqrt(dot((Vector) this));
  }

  @Override
  public double norm2Sqr() {
    return dot((Vector) this);
  }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("DenseVector(");
    try (Formatter formatter = new Formatter(b, Locale.US)) {
      final int size = Math.min(25, this.size());
      for (int i = 0; i < size; ++i) {
        if (i < size - 1) {
          formatter.format("%1.3f, ", this.get(i));
        } else {
          formatter.format("%1.3f ", this.get(i));
        }
      }
      if (this.size() > 25) {
        formatter.format("...");
      }
    }
    b.append(')');
    return b.toString();
  }

  @Override
  public Tuple<Integer, Double> min() {
    double min = get(0);
    int minIdx = 0;
    for (int i = 1; i < this.size(); ++i) {
      final double curVal = get(i);
      if (curVal < min) {
        min = curVal;
        minIdx = i;
      }
    }
    return new Tuple<Integer, Double>(minIdx, min);
  }
}
