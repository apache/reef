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
package org.apache.reef.wake.remote.ports;

import net.jcip.annotations.ThreadSafe;

import java.util.Iterator;
import java.util.Random;

/**
 *
 */
@ThreadSafe
class RandomRangeIterator implements Iterator<Integer> {
  private final int tcpPortRangeBegin;
  private final int tcpPortRangeCount;
  private final int tryCount;
  private int currentRetryCount;
  private final Random random = new Random(System.currentTimeMillis());

  RandomRangeIterator(final int tcpPortRangeBegin, final int tcpPortRangeCount, int tryCount) {
    this.tcpPortRangeBegin = tcpPortRangeBegin;
    this.tcpPortRangeCount = tcpPortRangeCount;
    this.tryCount = tryCount;
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   * (In other words, returns {@code true} if {@link #next} would
   * return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override
  public synchronized boolean hasNext() {
    return currentRetryCount++ < tryCount;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override
  public synchronized Integer next() {
    return random.nextInt(tcpPortRangeCount) + tcpPortRangeBegin;
  }

  /**
   * always throws
   * @throws UnsupportedOperationException if the {@code remove}
   *                                       operation is not supported by this iterator
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException ();
  }

}
