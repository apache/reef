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

/**
 * Input of {@link MatMulFunction} which contains two matrices to multiply,
 * and index of the sub-matrix in the entire result.
 */
final class MatMulInput {
  private int index;
  private Matrix<Double> leftMatrix;
  private Matrix<Double> rightMatrix;

  /**
   * No-arg constructor required for Kryo to ser/des.
   */
  MatMulInput() {
  }

  /**
   * Constructor of MatMulInput which consists of two matrices.
   * @param index Index of the resulting sub-matrix in the entire matrix.
   * @param leftMatrix Matrix to multiply on the left side.
   * @param rightMatrix Matrix to multiply on the right side.
   */
  MatMulInput(final int index, final Matrix<Double> leftMatrix, final Matrix<Double> rightMatrix) {
    this.index = index;
    this.leftMatrix = leftMatrix;
    this.rightMatrix = rightMatrix;
  }

  /**
   * @return Index of the resulting sub-matrix in the entire matrix.
   */
  int getIndex() {
    return index;
  }

  /**
   * @return Matrix to multiply on the left side.
   */
  Matrix<Double> getLeftMatrix() {
    return leftMatrix;
  }

  /**
   * @return Matrix to multiply on the right side.
   */
  Matrix<Double> getRightMatrix() {
    return rightMatrix;
  }
}
