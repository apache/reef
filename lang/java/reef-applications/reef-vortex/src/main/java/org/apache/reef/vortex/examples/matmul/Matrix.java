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

import java.util.List;

/**
 * Interface of Matrix.
 * @param <T> Type of elements in Matrix.
 */
interface Matrix<T> {

  /**
   * Add another matrix. Note that dimensions of two matrices should be identical.
   * @param matrix Another matrix to add.
   * @return Result of adding two matrices.
   * @throws MatMulException
   */
  Matrix<T> add(Matrix<T> matrix) throws MatMulException;

  /**
   * Multiply another matrix on the right. Note that the number of {@param matrix}'s columns
   * should be equal to the number of this matrix's rows.
   * @param matrix Another matrix to multiply.
   * @return Result of multiplying two matrices.
   */
  Matrix<T> multiply(Matrix<T> matrix) throws MatMulException;

  /**
   * Get the transpose of the matrix.
   * @return Result of transpose.
   */
  Matrix<T> transpose();

  /**
   * Get the rows of the matrix. It is highly recommended to return {@link java.util.Collections.UnmodifiableList}
   * for making the result immutable.
   * @return Rows of the matrix.
   */
  List<List<T>> getRows();

  /**
   * @return Number of rows.
   */
  int getNumRows();

  /**
   * @return Number of columns.
   */
  int getNumColumns();
}
