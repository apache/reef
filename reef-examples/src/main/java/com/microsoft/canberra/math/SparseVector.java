package com.microsoft.canberra.math;

/**
 * A sparse vector represented by an index and value array.
 */
public final class SparseVector extends AbstractImmutableVector {

  private final double[] values;
  private final int[] indices;
  private final int size;


  public SparseVector(final double[] values, final int[] indices, final int size) {
    this.values = values;
    this.indices = indices;
    this.size = size;
  }

  public SparseVector(final double[] values, final int[] indices) {
    this(values, indices, -1);
  }


  @Override
  public double get(final int index) {
    for (int i = 0; i < indices.length; ++i) {
      if (indices[i] == index) {
        return values[i];
      }
    }
    return 0;
  }

  @Override
  public int size() {
    return this.size;
  }
}
