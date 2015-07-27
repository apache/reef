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
package org.apache.reef.wake.remote.ports;

import net.jcip.annotations.ThreadSafe;

import java.util.Iterator;
import java.util.Random;

/**
 * This class will give out random port numbers between tcpPortRangeBegin and tcpPortRangeBegin+tcpPortRangeCount.
 * Max number of ports given is tryCount
 */
@ThreadSafe
final class RandomRangeIterator implements Iterator<Integer> {
  private final int tcpPortRangeBegin;
  private final int tcpPortRangeCount;
  private final int tryCount;
  private int currentRetryCount;
  private final Random random = new Random(System.currentTimeMillis());

  RandomRangeIterator(final int tcpPortRangeBegin, final int tcpPortRangeCount, final int tryCount) {
    this.tcpPortRangeBegin = tcpPortRangeBegin;
    this.tcpPortRangeCount = tcpPortRangeCount;
    this.tryCount = tryCount;
  }

  @Override
  public synchronized boolean hasNext() {
    return currentRetryCount++ < tryCount;
  }

  @Override
  public synchronized Integer next() {
    return random.nextInt(tcpPortRangeCount) + tcpPortRangeBegin;
  }

  /**
   * always throws.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
