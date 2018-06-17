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


import org.apache.commons.lang.StringUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.parameters.TcpPortList;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A TcpPortProvider which gives out random ports in a range.
 */
public final class ListTcpPortProvider implements TcpPortProvider {

  private static final Logger LOG = Logger.getLogger(ListTcpPortProvider.class.getName());
  private final List<Integer> tcpPortList;

  @Inject
  public ListTcpPortProvider(@Parameter(TcpPortList.class) final List<Integer> tcpPortList) {
    this.tcpPortList = tcpPortList;
    LOG.log(Level.FINE, "Instantiating {0}", this);
  }

  @Inject
  public ListTcpPortProvider(@Parameter(TcpPortList.class) final String tcpPortList) {
    this.tcpPortList = new ArrayList<>();
    String[] ports = StringUtils.split(tcpPortList, TcpPortList.SEPARATOR);
    for (int i = 0; i < ports.length; i++) {
      this.tcpPortList.add(Integer.parseInt(ports[i]));
    }
  }

  /**
   * Returns an iterator over a set of tcp ports.
   *
   * @return an Iterator.
   */
  @Override
  public Iterator<Integer> iterator() {
    return this.tcpPortList.iterator();
  }

  @Override
  public String toString() {
    return "ListTcpPortProvider{" + StringUtils.join(this.tcpPortList, ',') + '}';
  }
}
