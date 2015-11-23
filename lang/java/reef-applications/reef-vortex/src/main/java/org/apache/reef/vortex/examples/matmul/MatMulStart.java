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

import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MatMul User Code Example.
 * This example multiplies two matrices by distributing computation to multiple Tasklets.
 * Each Tasklet receives split of the matrix on the left side, and copy of the matrix on the right side.
 * To check whether the results are correct, Identity matrix is used for the matrix on the right side.
 */
final class MatMulStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(MatMulStart.class.getName());
  private static final int DIVIDE_FACTOR = 10000;
  private static final int NUM_ROWS = 100000;
  private static final int NUM_COLUMNS = 10;

  @Inject
  private MatMulStart() {
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {

    final Matrix<Double> left = generateRandomMatrix(NUM_ROWS, NUM_COLUMNS);
    final List<Matrix<Double>> leftSplits = split(left, DIVIDE_FACTOR);
    final Matrix<Double> right = generateIdentityMatrix(NUM_COLUMNS);

    // Measure job finish time starting from here..
    final double start = System.currentTimeMillis();

    // Define callback that is invoked when Tasklets finish.
    final CountDownLatch latch = new CountDownLatch(DIVIDE_FACTOR);
    final EventHandler<MatMulOutput> callback = new EventHandler<MatMulOutput>() {
      @Override
      public void onNext(final MatMulOutput output) {
        final int index = output.getIndex();
        final Matrix<Double> result = output.getResult();
        // Compare the result from the original matrix.
        if (result.equals(leftSplits.get(index))) {
          latch.countDown();
        } else {
          throw new RuntimeException(index + " th result is not correct.");
        }
      }
    };

    // Submit Tasklets and register callback.
    final MatMulFunction matMulFunction = new MatMulFunction();
    for (int i = 0; i < DIVIDE_FACTOR; i++) {
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
   * @param numRows number of matrix's rows.
   * @param numColumns number of matrix's columns.
   * @return Matrix that consists of random values.
   */
  private Matrix<Double> generateRandomMatrix(final int numRows, final int numColumns) {
    final List<Vector<Double>> vectors = new ArrayList<>(numRows);
    final Random random = new Random();
    for (int i = 0; i < numRows; i++) {
      final Vector<Double> vector = new Vector<>();
      for (int j = 0; j < numColumns; j++) {
        vector.add(random.nextDouble());
      }
      vectors.add(vector);
    }
    return new RowMatrix(vectors);
  }

  /**
   * Generate an identity matrix.
   * @param numDimension number of rows and columns of the identity matrix.
   * @return Identity matrix.
   */
  private Matrix<Double> generateIdentityMatrix(final int numDimension) {
    final List<Vector<Double>> vectors = new ArrayList<>(numDimension);
    for (int i = 0; i < numDimension; i++) {
      final Vector<Double> vector = new Vector<>();
      for (int j = 0; j < numDimension; j++) {
        final double value = i == j ? 1 : 0;
        vector.add(value);
      }
      vectors.add(vector);
    }
    return new RowMatrix(vectors);
  }

  /**
   * Split a matrix into sub-matrices as many as {@param divideFactor}.
   * Note that the matrix is split in row-wise, so the number of columns remain same while
   * the number of rows is divided by {@param divideFactor}.
   * @param matrix Matrix to Split.
   * @param divideFactor Number of partitions to split the matrix into.
   * @return List of matrices divided into multiple sub-matrices.
   */
  private List<Matrix<Double>> split(final Matrix<Double> matrix, final int divideFactor) {
    final List<Matrix<Double>> result = new ArrayList<>(divideFactor);
    final int totalNumRows = matrix.getNumRows();
    final int splitNumRows = (totalNumRows + divideFactor - 1) / divideFactor;

    int rowIndex = 0;
    for (int i = 0; i < divideFactor; i++) {
      final List<Vector<Double>> vectors = new ArrayList<>(splitNumRows);
      // Fill each split, but terminate once there is no more data.
      for (int j = 0; rowIndex < totalNumRows && j < splitNumRows; j++) {
        vectors.add(matrix.getRows().get(rowIndex));
        rowIndex++;
      }
      result.add(new RowMatrix(vectors));
    }
    return result;
  }
}
