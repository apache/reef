package com.microsoft.canberra.math;

import com.microsoft.reef.io.Tuple;

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
  public double get(int i);

  /**
   * The size (dimensionality) of the Vector
   *
   * @return the size of the Vector.
   */
  public int size();

  /**
   * Computes the inner product with another Vector.
   *
   * @param that
   * @return the inner product between two Vectors.
   */
  public double dot(Vector that);

  /**
   * Computes the computeSum of all entries in the Vector.
   *
   * @return the computeSum of all entries in this Vector
   */
  public double sum();

  /**
   * Computes the L2 norm of this Vector.
   *
   * @return the L2 norm of this Vector.
   */
  public double norm2();

  /**
   * Computes the min of all entries in the Vector
   *
   * @return the min of all entries in this Vector
   */
  public Tuple<Integer, Double> min();
}
