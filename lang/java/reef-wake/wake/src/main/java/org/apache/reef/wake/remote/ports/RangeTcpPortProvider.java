/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.remote.ports;


import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A TcpPortProvider which gives out random ports in a range.
 */
public final class RangeTcpPortProvider implements TcpPortProvider {
  private final int portRangeBegin;
  private final int portRangeCount;
  private final int portRangeTryCount;
  private static final Logger LOG = Logger.getLogger(RangeTcpPortProvider.class.getName());

  @Inject
  public RangeTcpPortProvider(@Parameter(TcpPortRangeBegin.class) final int portRangeBegin,
                              @Parameter(TcpPortRangeCount.class) final int portRangeCount,
                              @Parameter(TcpPortRangeTryCount.class) final int portRangeTryCount) {
    this.portRangeBegin = portRangeBegin;
    this.portRangeCount = portRangeCount;
    this.portRangeTryCount = portRangeTryCount;
    LOG.log(Level.FINE, "Instantiating " + this);
  }

  /**
   * Returns an iterator over a set of tcp ports.
   *
   * @return an Iterator.
   */
  @Override
  public Iterator<Integer> iterator() {
    return new RandomRangeIterator(portRangeBegin, portRangeCount, portRangeTryCount);
  }

  @Override
  public String toString() {
    return "RangeTcpPortProvider{" +
        "portRangeBegin=" + portRangeBegin +
        ", portRangeCount=" + portRangeCount +
        ", portRangeTryCount=" + portRangeTryCount +
        '}';
  }
}
