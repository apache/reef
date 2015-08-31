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
package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for running HelloREEF with tcp port configuration on YARN.
 */
public final class HelloReefYarnTcp {

  private static final Logger LOG = Logger.getLogger(HelloReefYarnTcp.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 150000; // 30 sec.

  private static final int DEFAULT_TCP_BEGIN_PORT = 8900;
  private static final int DEFAULT_TCP_RANGE_COUNT = 10;
  private static final int DEFAULT_TCP_RANGE_TRY_COUNT = 1111;

  /**
   * @param tcpBeginPort  the first tcp port number to try
   * @param tcpRangeCount the number of tcp ports in the range
   * @param tcpTryCount maximum number of tries for port numbers
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration(
      final int tcpBeginPort,
      final int tcpRangeCount,
      final int tcpTryCount) {

    return Tang.Factory.getTang().newConfigurationBuilder(YarnClientConfiguration.CONF.build())
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .build();
  }

  /**
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            HelloReefYarnTcp.class.getProtectionDomain().getCodeSource().getLocation().getFile())
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Start Hello REEF job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final int tcpBeginPort = args.length > 0 ? Integer.valueOf(args[0]) : DEFAULT_TCP_BEGIN_PORT;
    final int tcpRangeCount = args.length > 1 ? Integer.valueOf(args[1]) : DEFAULT_TCP_RANGE_COUNT;
    final int tcpTryCount = args.length > 2 ? Integer.valueOf(args[2]) : DEFAULT_TCP_RANGE_TRY_COUNT;

    final Configuration runtimeConf = getRuntimeConfiguration(tcpBeginPort, tcpRangeCount, tcpTryCount);
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloReefYarnTcp() {
  }
}
