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

/**
 * Histogram
 */
public interface Histogram {

  /**
   * Updates the value in this histogram
   *
   * @param value the new value
   */
  public void update(long value);

  /**
   * Returns the number of recorded values
   *
   * @return the number of recorded values
   */
  public long getCount();

  /**
   * Returns the value of the index
   *
   * @param index the histogram bin index
   * @return the value of the index
   * @throws IndexOutOfBoundsException
   */
  public long getValue(int index);

  /**
   * Returns the number of bins
   *
   * @return the number of bins
   */
  public int getNumBins();
}
