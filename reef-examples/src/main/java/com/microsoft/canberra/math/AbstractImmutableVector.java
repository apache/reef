package com.microsoft.canberra.math;

import com.microsoft.reef.io.Tuple;

import java.util.Formatter;
import java.util.Locale;

/**
 * Base class for implementing ImmutableVector
 */
abstract class AbstractImmutableVector implements ImmutableVector {

  @Override
  public abstract double get(int i);

  @Override
  public abstract int size();

  @Override
  public double dot(final Vector that) {
    assert (this.size() == that.size());

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
    double result = 0.0;
    for (int i = 0; i < this.size(); ++i) {
      result += Math.pow(this.get(i), 2.0);
    }
    return Math.sqrt(result);
  }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("DenseVector(");
    try (final Formatter formatter = new Formatter(b, Locale.US)) {
      for (int i = 0; i < this.size() - 1; ++i) {
        formatter.format("%1.3f, ", this.get(i));
      }
      formatter.format("%1.3f", this.get(this.size() - 1));
    }
    b.append(')');
    return b.toString();
  }

  @Override
  public Tuple<Integer, Double> min() {
    double min = get(0);
    int minIdx = 0;
    for (int i = 1; i < this.size(); ++i) {
      double curVal = get(i);
      if (curVal < min) {
        min = curVal;
        minIdx = i;
      }
    }
    return new Tuple<Integer, Double>(minIdx, min);
  }
}
