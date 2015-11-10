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

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.Validate;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.driver.parameters.MaxApplicationSubmissions;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroYarnClusterJobSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a job submission from the CS code.
 * <p>
 * This class exists mostly to parse and validate the command line parameters provided by the C# class
 * `Org.Apache.REEF.Client.YARN.YARNClient`
 */
final class YarnSubmissionFromCS {
  private static final int DEFAULT_PRIORITY = 1;
  private static final String DEFAULT_QUEUE = "default";

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
  private final String tokenKind;
  private final String tokenService;
  private final String jobSubmissionDirectoryPrefix;

  private YarnSubmissionFromCS(final AvroYarnClusterJobSubmissionParameters yarnClusterJobSubmissionParameters) {
    final AvroYarnJobSubmissionParameters yarnJobSubmissionParameters =
        yarnClusterJobSubmissionParameters.getYarnJobSubmissionParameters();

    final AvroJobSubmissionParameters jobSubmissionParameters =
        yarnJobSubmissionParameters.getSharedJobSubmissionParameters();

    this.driverFolder = new File(jobSubmissionParameters.getJobSubmissionFolder().toString());
    this.jobId = jobSubmissionParameters.getJobId().toString();
    this.tcpBeginPort = jobSubmissionParameters.getTcpBeginPort();
    this.tcpRangeCount = jobSubmissionParameters.getTcpRangeCount();
    this.tcpTryCount = jobSubmissionParameters.getTcpTryCount();
    this.maxApplicationSubmissions = yarnClusterJobSubmissionParameters.getMaxApplicationSubmissions();
    this.driverRecoveryTimeout = yarnJobSubmissionParameters.getDriverRecoveryTimeout();
    this.driverMemory = yarnJobSubmissionParameters.getDriverMemory();
    this.priority = DEFAULT_PRIORITY;
    this.queue = DEFAULT_QUEUE;
    this.tokenKind = yarnClusterJobSubmissionParameters.getSecurityTokenKind().toString();
    this.tokenService = yarnClusterJobSubmissionParameters.getSecurityTokenService().toString();
    this.jobSubmissionDirectoryPrefix = yarnJobSubmissionParameters.getJobSubmissionDirectoryPrefix().toString();

    Validate.isTrue(driverFolder.exists(), "The driver folder given does not exist.");
    Validate.notEmpty(jobId, "The job id is null or empty");
    Validate.isTrue(driverMemory > 0, "The amount of driver memory given is <= 0.");
    Validate.isTrue(tcpBeginPort >= 0, "The tcp start port given is < 0.");
    Validate.isTrue(tcpRangeCount > 0, "The tcp range given is <= 0.");
    Validate.isTrue(tcpTryCount > 0, "The tcp retry count given is <= 0.");
    Validate.isTrue(maxApplicationSubmissions > 0, "The maximum number of app submissions given is <= 0.");
    Validate.notEmpty(queue, "The queue is null or empty");
    Validate.notEmpty(tokenKind, "Token kind should be either NULL or some custom non empty value");
    Validate.notEmpty(tokenService, "Token service should be either NULL or some custom non empty value");
    Validate.notEmpty(jobSubmissionDirectoryPrefix, "Job submission directory prefix should not be empty");
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
        ", tokenKind='" + tokenKind + '\'' +
        ", tokenService='" + tokenService + '\'' +
        ", jobSubmissionDirectoryPrefix='" + jobSubmissionDirectoryPrefix + '\'' +
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
        .bindNamedParameter(JobSubmissionDirectoryPrefix.class, jobSubmissionDirectoryPrefix)
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
   * @return The security token kind
   */
  String getTokenKind() {
    return tokenKind;
  }

  /**
   * @return The security token service
   */
  String getTokenService() {
    return tokenService;
  }

  /**
   * Takes the YARN cluster job submission configuration file, deserializes it, and creates submission object.
   */
  static YarnSubmissionFromCS fromJobSubmissionParametersFile(final File yarnClusterJobSubmissionParametersFile)
      throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(yarnClusterJobSubmissionParametersFile)) {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          AvroYarnClusterJobSubmissionParameters.getClassSchema(), fileInputStream);
      final SpecificDatumReader<AvroYarnClusterJobSubmissionParameters> reader = new SpecificDatumReader<>(
          AvroYarnClusterJobSubmissionParameters.class);
      final AvroYarnClusterJobSubmissionParameters yarnClusterJobSubmissionParameters = reader.read(null, decoder);

      return new YarnSubmissionFromCS(yarnClusterJobSubmissionParameters);
    }
  }
}
