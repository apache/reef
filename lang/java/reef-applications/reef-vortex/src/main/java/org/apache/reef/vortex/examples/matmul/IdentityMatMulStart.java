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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MatMul User Code Example.
 * This example multiplies two matrices by distributing computation to multiple Tasklets.
 * Each Tasklet receives split of the matrix on the left side, and copy of the matrix on the right side.
 * To check whether the result is correct, Identity matrix is multiplied on the right side.
 */
final class IdentityMatMulStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(IdentityMatMulStart.class.getName());

  private final int divideFactor;
  private final int numRows;
  private final int numColumns;

  @Inject
  private IdentityMatMulStart(@Parameter(MatMul.DivideFactor.class) final int divideFactor,
                              @Parameter(MatMul.NumRows.class) final int numRows,
                              @Parameter(MatMul.NumColumns.class) final int numColumns) {
    this.divideFactor = divideFactor;
    this.numRows = numRows;
    this.numColumns = numColumns;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final List<Matrix<Double>> leftSplits = generateMatrixSplits(numRows, numColumns, divideFactor);
    final Matrix<Double> right = generateIdentityMatrix(numColumns);

    // Measure job finish time starting from here..
    final double start = System.currentTimeMillis();

    // Define callback that is invoked when Tasklets finish.
    final CountDownLatch latch = new CountDownLatch(divideFactor);
    final FutureCallback<MatMulOutput> callback = new FutureCallback<MatMulOutput>() {
      @Override
      public void onSuccess(final MatMulOutput output) {
        final int index = output.getIndex();
        final Matrix<Double> result = output.getResult();
        // Compare the result from the original matrix.
        if (result.equals(leftSplits.get(index))) {
          latch.countDown();
        } else {
          throw new RuntimeException(index + " th result is not correct.");
        }
      }

      @Override
      public void onFailure(final Throwable t) {
        throw new RuntimeException(t);
      }
    };

    // Submit Tasklets and register callback.
    final MatMulFunction matMulFunction = new MatMulFunction();
    for (int i = 0; i < divideFactor; i++) {
      vortexThreadPool.submit(matMulFunction, new MatMulInput(i, leftSplits.get(i), right), callback);
    }

    try {
      // Wait until all Tasklets finish.
      latch.await();
      LOG.log(Level.INFO, "Job Finish Time: " + (System.currentTimeMillis() - start));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate a matrix with random values.
   * @param nRows number of matrix's rows.
   * @param nColumns number of matrix's columns.
   * @return Matrix that consists of random values.
   */
  private Matrix<Double> generateRandomMatrix(final int nRows, final int nColumns) {
    final List<List<Double>> rows = new ArrayList<>(nRows);
    final Random random = new Random();
    for (int i = 0; i < nRows; i++) {
      final List<Double> row = new ArrayList<>(nColumns);
      for (int j = 0; j < nColumns; j++) {
        row.add(random.nextDouble());
      }
      rows.add(row);
    }
    return new RowMatrix(rows);
  }

  /**
   * Generate an identity matrix.
   * @param numDimension number of rows and columns of the identity matrix.
   * @return Identity matrix.
   */
  private Matrix<Double> generateIdentityMatrix(final int numDimension) {
    final List<List<Double>> rows = new ArrayList<>(numDimension);
    for (int i = 0; i < numDimension; i++) {
      final List<Double> row = new ArrayList<>(numDimension);
      for (int j = 0; j < numDimension; j++) {
        final double value = i == j ? 1 : 0;
        row.add(value);
      }
      rows.add(row);
    }
    return new RowMatrix(rows);
  }

  /**
   * Generate sub-matrices which splits a matrix as many as {@param nSplits}.
   * Note that the matrix is split in row-wise, so the number of columns remain same while
   * the number of rows is divided by {@param nSplits}.
   * @param nRows Number of rows of the original Matrix.
   * @param nColumns Number of columns of the original Matrix.
   * @param nSplits Number of partitions to split the matrix into.
   * @return List of matrices divided into multiple sub-matrices.
   */
  private List<Matrix<Double>> generateMatrixSplits(final int nRows, final int nColumns, final int nSplits) {
    final List<Matrix<Double>> splits = new ArrayList<>(nSplits);

    int remainingNumSplits = nSplits;
    int remainingNumRows = nRows;
    for (int i = 0; i < nSplits; i++) {
      final int splitNumRows = (remainingNumRows + remainingNumSplits - 1) / remainingNumSplits;
      splits.add(generateRandomMatrix(splitNumRows, nColumns));

      remainingNumRows -= splitNumRows;
      remainingNumSplits--;
    }
    return splits;
  }
}
