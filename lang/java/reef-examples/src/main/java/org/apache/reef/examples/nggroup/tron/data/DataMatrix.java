/**
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
package org.apache.reef.examples.nggroup.tron.data;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.examples.nggroup.tron.data.parser.FeatureParser;
import org.apache.reef.examples.nggroup.tron.data.parser.SVMLightParser;
import org.apache.reef.examples.nggroup.utils.math.DenseVector;
import org.apache.reef.examples.nggroup.utils.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Useful for batch operations in linear solvers
 * where we want to express computation as matrix
 * products & matrix' products with a vector
 */
public class DataMatrix implements org.apache.reef.examples.nggroup.tron.data.DataSet {

  private static final int NUM_THREADS = 2;

  private static final int DATALOAD_REPORTING_INTERVAL = 2000;

  private static final Logger LOG = Logger.getLogger(DataMatrix.class.getName());

  private final FeatureParser<String> parser;
  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  public DataMatrix(final DataSet<LongWritable, Text> dataSet, final FeatureParser<String> parser) {
    this.dataSet = dataSet;
    this.parser = parser;
  }

  private void loadData() {
    LOG.info("Loading data");
    int i = 0;
    for (final Pair<LongWritable, Text> examplePair : dataSet) {
      final Example example = parser.parse(examplePair.second.toString());
      examples.add(example);
      if (++i % DATALOAD_REPORTING_INTERVAL == 0) {
        LOG.log(Level.FINEST, "Done parsing {0} lines", i);
      }
    }
  }

  @Override
  public Iterator<Example> iterator() {
    if (examples.isEmpty()) {
      loadData();
    }
    return examples.iterator();
  }

  @Override
  public int getNumberOfExamples() {
    if (examples.isEmpty()) {
      loadData();
    }
    return examples.size();
  }

  public int getDimensionality() {
    if (examples.isEmpty()) {
      loadData();
    }
    return parser.getDimensionality();
  }

  public void times(final Vector vector, final Vector result) {
    if (examples.isEmpty()) {
      loadData();
    }
    final int numberOfExamples = getNumberOfExamples();
    assert (result.size() == numberOfExamples);
    assert (vector.size() == getDimensionality());
    final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
    final EventHandler<Pair<Integer, Integer>> timesHandler = new EventHandler<Pair<Integer, Integer>>() {

      @Override
      public void onNext(final Pair<Integer, Integer> range) {
        for (int i = range.first; i < range.second; i++) {
          final Example ex = examples.get(i);
          result.set(i, ex.predict(vector));
        }
        latch.countDown();
      }
    };
    final ThreadPoolStage<Pair<Integer, Integer>> stage = new ThreadPoolStage<>(timesHandler, NUM_THREADS);
    distributeEvenly(numberOfExamples, NUM_THREADS, timesHandler, stage);

    try {
      latch.await();
      stage.close();
    } catch (final Exception e) {
      throw new RuntimeException("Exception while waiting for multi-threaded multiplication to happen", e);
    }
  }

  public void transposeTimes(final Vector vector, final Vector result) {
    assert (result.size() == getDimensionality());
    assert (vector.size() == getNumberOfExamples());
    if (examples.isEmpty()) {
      loadData();
    }
    final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
    final Object resultLock = new Object();
    final EventHandler<Pair<Integer, Integer>> transposeTimesHandler = new EventHandler<Pair<Integer, Integer>>() {

      @Override
      public void onNext(final Pair<Integer, Integer> range) {
        final Vector intResult = result.newInstance();
        for (int i = range.first; i < range.second; i++) {
          final Example ex = examples.get(i);
          ex.addToVector(intResult, vector.get(i));
        }
        synchronized (resultLock) {
          result.add(intResult);
        }
        latch.countDown();
      }
    };
    final ThreadPoolStage<Pair<Integer, Integer>> stage = new ThreadPoolStage<>(transposeTimesHandler, NUM_THREADS);
    distributeEvenly(getNumberOfExamples(), NUM_THREADS, transposeTimesHandler, stage);

    try {
      latch.await();
      stage.close();
    } catch (final Exception e) {
      throw new RuntimeException("Exception while waiting for multi-threaded transpose multiplication to happen", e);
    }
  }

  private void distributeEvenly(final int numberOfExamples, final int numThreads, final EventHandler<Pair<Integer, Integer>> timesHandler, final ThreadPoolStage<Pair<Integer, Integer>> stage) {
    final int stepSize = numberOfExamples / numThreads;
    final int remainder = numberOfExamples % numThreads;
    int offset = 0;
    for (int i = 0; i < remainder; i++) {
      stage.onNext(new Pair<>(offset, offset + stepSize + 1));
      offset += stepSize + 1;
    }

    for (int i = remainder; i < numThreads; i++) {
      stage.onNext(new Pair<>(offset, offset + stepSize));
      offset += stepSize;
    }
  }

  public static void main(final String[] args) {
    final ExampleDataSet ds = new ExampleDataSet();
    final DataMatrix X = new DataMatrix(ds, new SVMLightParser());
    final int dim = X.getDimensionality();
    assert (dim == 3);
    LOG.log(Level.INFO, "OUT: Dimensions: {0}", dim);
    final int numEx = X.getNumberOfExamples();
    assert (numEx == 10);
    LOG.log(Level.INFO, "OUT: #Examples: {0}", numEx);
    final DenseVector vector = new DenseVector(new double[]{1, 1, 1});
    final DenseVector result = new DenseVector(numEx);
    X.times(vector, result);
    LOG.log(Level.INFO, "OUT: result: {0}", result);
    final DenseVector tVector = new DenseVector(new double[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    final DenseVector tResult = new DenseVector(dim);
    X.transposeTimes(tVector, tResult);
    LOG.log(Level.INFO, "OUT: tResult: {0}", tResult);
  }
}

class ExampleDataSet implements DataSet<LongWritable, Text> {

  List<Pair<LongWritable, Text>> data = new ArrayList<>();

  /**
   * 2 0 0
   * 0 3 0
   * 0 0 4
   * 2 3 0
   * 0 3 4
   * 4 0 5
   * 2 3 0
   * 0 3 4
   * 4 0 5
   * 2 3 4
   */
  public ExampleDataSet() {
    final LongWritable l = new LongWritable(0);
    data.add(new Pair<>(l, new Text("1 1:2")));
    data.add(new Pair<>(l, new Text("1 2:3")));
    data.add(new Pair<>(l, new Text("1 3:4")));
    data.add(new Pair<>(l, new Text("1 1:2 2:3")));
    data.add(new Pair<>(l, new Text("1 2:3 3:4")));
    data.add(new Pair<>(l, new Text("1 1:4 3:5")));
    data.add(new Pair<>(l, new Text("1 1:2 2:3")));
    data.add(new Pair<>(l, new Text("1 2:3 3:4")));
    data.add(new Pair<>(l, new Text("1 1:4 3:5")));
    data.add(new Pair<>(l, new Text("1 1:2 2:3 3:4")));
  }

  @Override
  public Iterator<Pair<LongWritable, Text>> iterator() {
    return data.iterator();
  }
}
