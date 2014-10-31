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
package org.apache.reef.wake.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * An {@link Histogram} that implements uniform binning of numbers (>=0)
 */
public class UniformHistogram implements Histogram {
  private final AtomicLong count;
  private final AtomicLongArray values;
  private final long binWidth;
  private final int numBins;

  /**
   * Constructs a histogram
   *
   * @param binWidth the width of each bin
   * @param numBins  the number of bins
   */
  public UniformHistogram(long binWidth, int numBins) {
    this.count = new AtomicLong(0);
    this.values = new AtomicLongArray(numBins);
    this.binWidth = binWidth;
    this.numBins = numBins;
  }

  /**
   * Updates the value
   *
   * @param value
   */
  @Override
  public void update(long value) {
    count.incrementAndGet();
    int index = (int) (value / binWidth);
    if (index >= numBins)
      index = numBins - 1;
    values.incrementAndGet(index);
  }

  /**
   * Returns the number of recorded values
   *
   * @return the number of recorded values
   */
  @Override
  public long getCount() {
    return count.get();
  }

  /**
   * Returns the value of the index
   *
   * @param index the index
   * @return the value of the index
   */
  @Override
  public long getValue(int index) {
    return values.get(index);
  }

  /**
   * Returns the number of bins
   *
   * @return the number of bins
   */
  @Override
  public int getNumBins() {
    return numBins;
  }
}
