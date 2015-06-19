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
package org.apache.reef.io;

import org.apache.reef.driver.parameters.EvaluatorConfigurationProviders;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationProvider;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements ConfigurationProvider for RangeTcpPortProvider.
 */
public class TcpPortConfigurationProvider implements ConfigurationProvider {
  private final int portRangeBegin;
  private final int portRangeCount;
  private final int portRangeTryCount;
  private static final Logger LOG = Logger.getLogger(TcpPortConfigurationProvider.class.getName());

  @Inject
  TcpPortConfigurationProvider(@Parameter(TcpPortRangeBegin.class) final int portRangeBegin,
                               @Parameter(TcpPortRangeCount.class) final int portRangeCount,
                               @Parameter(TcpPortRangeTryCount.class) final int portRangeTryCount) {
    this.portRangeBegin = portRangeBegin;
    this.portRangeCount = portRangeCount;
    this.portRangeTryCount = portRangeTryCount;
    LOG.log(Level.INFO, "Instantiating " + this.toString());
  }

  /**
   * returns a configuration for the class that implements TcpPortProvider so that class can be instantiated.
   * somewhere else
   *
   * @return Configuration.
   */
  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(TcpPortRangeBegin.class, String.valueOf(portRangeBegin))
        .bindNamedParameter(TcpPortRangeCount.class, String.valueOf(portRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, String.valueOf(portRangeTryCount))
        .bindSetEntry(EvaluatorConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .build();
  }

  @Override
  public String toString() {
    return "TcpPortConfigurationProvider{" +
        "portRangeBegin=" + portRangeBegin +
        ", portRangeCount=" + portRangeCount +
        ", portRangeTryCount=" + portRangeTryCount +
        '}';
  }
}
