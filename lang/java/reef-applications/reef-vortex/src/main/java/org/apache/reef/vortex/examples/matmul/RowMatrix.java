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
package org.apache.reef.vortex.examples.matmul;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Row-oriented matrix implementation used in {@link MatMul} example.
 */
final class RowMatrix implements Matrix<Double> {
  private final List<Vector<Double>> values;

  /**
   * Constructor of matrix which creates an empty matrix of size (numRow x numColumn).
   * @param values Elements of Matrix.
   */
  RowMatrix(final List<Vector<Double>> values) {
    this.values = values;
  }

  @Override
  public Matrix<Double> add(final Matrix<Double> matrix) throws MatMulException {
    if (this.getNumRows() != matrix.getNumRows() || this.getNumColumns() != matrix.getNumColumns()) {
      throw new MatMulException("The dimension of two matrices should be same to add.");
    }
    final List<Vector<Double>> result = new ArrayList<>(getNumRows());
    final Matrix<Double> transpose = transpose();
    for (int i = 0; i < getNumRows(); i++) {
      final Vector<Double> row1 = getRows().get(i);
      final Vector<Double> row2 = transpose.getRows().get(i);
      for (int j = 0; j < getNumColumns(); j++) {
        result.get(i).add(row1.get(j) + row2.get(j));
      }
    }
    return new RowMatrix(result);
  }

  @Override
  public Matrix<Double> multiply(final Matrix<Double> matrix) throws MatMulException {
    if (this.getNumRows() != matrix.getNumColumns()) {
      throw new MatMulException("The number of columns of matrix to multiply should be same to the number of rows.");
    }
    final List<Vector<Double>> result = new ArrayList<>(getNumRows());
    for (int i = 0; i < getNumRows(); i++) {
      result.add(new Vector<Double>(matrix.getNumColumns()));
    }

    // result(i, j) = leftMatrix.row(i) * rightMatrix.col(j)
    final Matrix<Double> transpose = matrix.transpose();
    for (int i = 0; i < getNumRows(); i++) {
      final Vector<Double> row = getRows().get(i);

      for (int j = 0; j < getNumColumns(); j++) {
        final Vector<Double> col = transpose.getRows().get(j);
        result.get(i).add(dot(row, col));
      }
    }
    return new RowMatrix(result);
  }

  @Override
  public Matrix<Double> transpose() {
    // Initialize empty vectors.
    final ArrayList<Vector<Double>> transpose = new ArrayList<>(getNumColumns());
    for (int i = 0; i < getNumRows(); i++) {
      transpose.add(new Vector<Double>(getNumRows()));
    }

    // Each element in rows is added to corresponding column in transpose matrix.
    for (final Vector<Double> row : getRows()) {
      for (int i = 0; i < row.size(); i++) {
        transpose.get(i).add(row.get(i));
      }
    }
    return new RowMatrix(transpose);
  }

  @Override
  public List<Vector<Double>> getRows() {
    return values;
  }

  @Override
  public int getNumRows() {
    return values.size();
  }

  @Override
  public int getNumColumns() {
    return values.get(0).size();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RowMatrix rowMatrix = (RowMatrix) o;

    return !(values != null ? !values.equals(rowMatrix.values) : rowMatrix.values != null);
  }

  @Override
  public int hashCode() {
    return values != null ? values.hashCode() : 0;
  }

  /**
   * @return Inner product of two vectors.
   */
  private double dot(final Vector<Double> vector1, final Vector<Double> vector2) throws MatMulException {
    if (vector1.size() != vector2.size()) {
      throw new MatMulException("The dimension of vectors should be equal.");
    }

    double result = 0.0;
    for (int i = 0; i < vector1.size(); i++) {
      result += vector1.get(i) * vector2.get(i);
    }
    return result;
  }
}
