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
package org.apache.reef.bridge.client;

import org.apache.commons.lang.Validate;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.driver.parameters.MaxApplicationSubmissions;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a job submission from the CS code.
 * <p/>
 * This class exists mostly to parse and validate the command line parameters provided by the C# class
 * `Org.Apache.REEF.Client.YARN.YARNClient`
 */
final class YarnSubmissionFromCS {
  private final File driverFolder;
  private final String jobId;
  private final int driverMemory;
  private final int tcpBeginPort;
  private final int tcpRangeCount;
  private final int tcpTryCount;
  private final int maxApplicationSubmissions;
  private final int driverRecoveryTimeout;
  // Static for now
  private final int priority;
  private final String queue;

  private YarnSubmissionFromCS(final File driverFolder,
                               final String jobId,
                               final int driverMemory,
                               final int tcpBeginPort,
                               final int tcpRangeCount,
                               final int tcpTryCount,
                               final int maxApplicationSubmissions,
                               final int driverRecoveryTimeout,
                               final int priority,
                               final String queue) {

    Validate.isTrue(driverFolder.exists(), "The driver folder given does not exist.");
    Validate.notEmpty(jobId, "The job id is null or empty");
    Validate.isTrue(driverMemory > 0, "The amount of driver memory given is <= 0.");
    Validate.isTrue(tcpBeginPort >= 0, "The tcp start port given is < 0.");
    Validate.isTrue(tcpRangeCount > 0, "The tcp range given is <= 0.");
    Validate.isTrue(tcpTryCount > 0, "The tcp retry count given is <= 0.");
    Validate.isTrue(maxApplicationSubmissions > 0, "The maximum number of app submissions given is <= 0.");
    Validate.notEmpty(queue, "The queue is null or empty");

    this.driverFolder = driverFolder;
    this.jobId = jobId;
    this.driverMemory = driverMemory;
    this.tcpBeginPort = tcpBeginPort;
    this.tcpRangeCount = tcpRangeCount;
    this.tcpTryCount = tcpTryCount;
    this.maxApplicationSubmissions = maxApplicationSubmissions;
    this.driverRecoveryTimeout = driverRecoveryTimeout;
    this.priority = priority;
    this.queue = queue;
  }

  @Override
  public String toString() {
    return "YarnSubmissionFromCS{" +
        "driverFolder=" + driverFolder +
        ", jobId='" + jobId + '\'' +
        ", driverMemory=" + driverMemory +
        ", tcpBeginPort=" + tcpBeginPort +
        ", tcpRangeCount=" + tcpRangeCount +
        ", tcpTryCount=" + tcpTryCount +
        ", maxApplicationSubmissions=" + maxApplicationSubmissions +
        ", driverRecoveryTimeout=" + driverRecoveryTimeout +
        ", priority=" + priority +
        ", queue='" + queue + '\'' +
        '}';
  }

  /**
   * Produces the YARN Runtime Configuration based on the parameters passed from C#.
   *
   * @return the YARN Runtime Configuration based on the parameters passed from C#.
   */
  Configuration getRuntimeConfiguration() {
    final Configuration providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .build();

    final List<String> driverLaunchCommandPrefixList = new ArrayList<>();
    driverLaunchCommandPrefixList.add(new REEFFileNames().getDriverLauncherExeFile().toString());

    final Configuration yarnJobSubmissionClientParamsConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(SubmissionDriverRestartEvaluatorRecoverySeconds.class,
            Integer.toString(driverRecoveryTimeout))
        .bindNamedParameter(MaxApplicationSubmissions.class, Integer.toString(maxApplicationSubmissions))
        .bindList(DriverLaunchCommandPrefix.class, driverLaunchCommandPrefixList)
        .build();

    return Configurations.merge(YarnClientConfiguration.CONF.build(), providerConfig,
        yarnJobSubmissionClientParamsConfig);
  }

  /**
   * @return The local folder where the driver is staged.
   */
  File getDriverFolder() {
    return driverFolder;
  }

  /**
   * @return the id of the job to be submitted.
   */
  String getJobId() {
    return jobId;
  }

  /**
   * @return the amount of memory to allocate for the Driver, in MB.
   */
  int getDriverMemory() {
    return driverMemory;
  }

  /**
   * @return The priority of the job submission
   */
  int getPriority() {
    return priority;
  }

  /**
   * @return The queue the driver will be submitted to.
   */
  String getQueue() {
    return queue;
  }

  /**
   * Takes 5 parameters from the C# side:
   * [0]: String. Driver folder.
   * [1]: String. Driver identifier.
   * [2]: int. Driver memory.
   * [3~5]: int. TCP configurations.
   * [6]: int. Max application submissions.
   * [7]: int. Evaluator recovery timeout for driver restart. > 0 => restart is enabled.
   */
  static YarnSubmissionFromCS fromCommandLine(final String[] args) {
    final File driverFolder = new File(args[0]);
    final String jobId = args[1];
    final int driverMemory = Integer.valueOf(args[2]);
    final int tcpBeginPort = Integer.valueOf(args[3]);
    final int tcpRangeCount = Integer.valueOf(args[4]);
    final int tcpTryCount = Integer.valueOf(args[5]);
    final int maxApplicationSubmissions = Integer.valueOf(args[6]);
    final int driverRecoveryTimeout = Integer.valueOf(args[7]);
    // Static for now
    final int priority = 1;
    final String queue = "default";
    return new YarnSubmissionFromCS(driverFolder, jobId, driverMemory, tcpBeginPort, tcpRangeCount, tcpTryCount,
        maxApplicationSubmissions, driverRecoveryTimeout, priority, queue);
  }
}
