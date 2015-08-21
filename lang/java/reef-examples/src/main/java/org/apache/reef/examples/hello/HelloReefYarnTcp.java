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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example.
 */
public final class HelloReefYarnTcp {

  private static final Text KIND = new Text("CaboSystemSecurityKeyToken");
  private static final Logger LOG = Logger.getLogger(HelloReefYarnTcp.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 150000; // 30 sec.

  private HelloReefYarnTcp(){}
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

  private static Configuration getRuntimeConfiguration(
      final int tcpBeginPort,
      final int tcpRangeCount,
      final int tcpTryCount,
      final String jobSubmissionDirectoryPrefix) {

    return Tang.Factory.getTang().newConfigurationBuilder(YarnClientConfiguration.CONF.build())
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .bindNamedParameter(JobSubmissionDirectoryPrefix.class, jobSubmissionDirectoryPrefix)
        .build();
  }

  private static void setupUserToken() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    byte[] key = Files.readAllBytes(Paths.get("D:\\anupam\\AzureSystemJmKeys"));
    Token token = new Token(key, "none".getBytes(), KIND, KIND);
    ugi.addToken(token);
  }

  /**
   * Start Hello REEF job. Runs method runHelloReef().
   * @param args command line parameters.
   * @throws org.apache.reef.tang.exceptions.BindException      configuration error.
   * @throws org.apache.reef.tang.exceptions.InjectionException configuration error.
   */
  public static final int DEFAULT_TCP_BEGIN_PORT = 8900;
  public static final int DEFAULT_TCP_RANGE_COUNT = 10;
  public static final int DEFAULT_TCP_RANGE_TRY_COUNT = 1111;
  public static final String DEFAULT_JOB_SUBMISSION_DIR_PREFIX = "/vol1/tmp";
  public static void main(final String[] args) throws InjectionException, IOException {
    final int tcpBeginPort = args.length > 0 ? Integer.valueOf(args[0]) : DEFAULT_TCP_BEGIN_PORT;
    final int tcpRangeCount = args.length > 1 ? Integer.valueOf(args[1]) : DEFAULT_TCP_RANGE_COUNT;
    final int tcpTryCount = args.length > 2 ? Integer.valueOf(args[2]) : DEFAULT_TCP_RANGE_TRY_COUNT;
    final String jobSubmissionDirectoryPrefix = args.length > 3 ? args[3] : DEFAULT_JOB_SUBMISSION_DIR_PREFIX;

    setupUserToken();
    Configuration runtimeConfiguration = getRuntimeConfiguration(
        tcpBeginPort, tcpRangeCount, tcpTryCount, jobSubmissionDirectoryPrefix);
    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConfiguration)
        .run(getDriverConfiguration(), JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
