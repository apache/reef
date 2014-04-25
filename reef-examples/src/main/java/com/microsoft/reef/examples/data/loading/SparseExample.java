package com.microsoft.reef.examples.data.loading;

import com.microsoft.canberra.math.Vector;

/**
 * Example implementation on a index and value array.
 */
public final class SparseExample implements Example {

  /**
   *
   */
  private static final long serialVersionUID = -2127500625316875426L;
  private final double[] values;
  private final int[] indices;
  private final double label;

  public SparseExample(final double label, final double[] values, final int[] indices) {
    this.label = label;
    this.values = values;
    this.indices = indices;

  }

  @Override
  public double getLabel() {
    return this.label;
  }

  @Override
  public double predict(final Vector w) {
    double result = 0.0;
    for (int i = 0; i < indices.length; ++i) {
      result += w.get(this.indices[i]) * this.values[i];
    }
    return result;
  }

  @Override
  public void addGradient(final Vector gradientVector, final double gradient) {
    for (int i = 0; i < this.indices.length; ++i) {
      final int index = this.indices[i];
      final double contribution = gradient * this.values[i];
      final double oldValue = gradientVector.get(index);
      final double newValue = oldValue + contribution;
      gradientVector.set(index, newValue);
    }
  }
}
