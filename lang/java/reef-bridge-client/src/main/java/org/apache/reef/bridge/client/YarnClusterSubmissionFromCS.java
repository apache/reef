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
import org.apache.reef.reef.bridge.client.avro.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a job submission from the CS code.
 * <p>
 * This class exists mostly to parse and validate the command line parameters provided by the C# class
 * `Org.Apache.REEF.Client.YARN.YARNClient`
 */
final class YarnClusterSubmissionFromCS {
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
  private final String fileSystemUrl;
  private final String yarnDriverStdoutFilePath;
  private final String yarnDriverStderrFilePath;
  private final Map<String, String> environmentVariablesMap = new HashMap<>();

  private final AvroYarnAppSubmissionParameters yarnAppSubmissionParameters;
  private final AvroYarnJobSubmissionParameters yarnJobSubmissionParameters;

  private YarnClusterSubmissionFromCS(final AvroYarnAppSubmissionParameters yarnAppSubmissionParameters,
                                      final AvroYarnClusterJobSubmissionParameters yarnClusterJobSubmissionParameters) {
    yarnJobSubmissionParameters = yarnClusterJobSubmissionParameters.getYarnJobSubmissionParameters();
    this.yarnAppSubmissionParameters = yarnAppSubmissionParameters;

    final AvroJobSubmissionParameters jobSubmissionParameters =
        yarnJobSubmissionParameters.getSharedJobSubmissionParameters();

    final AvroAppSubmissionParameters appSubmissionParameters =
        this.yarnAppSubmissionParameters.getSharedAppSubmissionParameters();

    this.driverFolder = new File(jobSubmissionParameters.getJobSubmissionFolder().toString());
    this.jobId = jobSubmissionParameters.getJobId().toString();
    this.tcpBeginPort = appSubmissionParameters.getTcpBeginPort();
    this.tcpRangeCount = appSubmissionParameters.getTcpRangeCount();
    this.tcpTryCount = appSubmissionParameters.getTcpTryCount();
    this.maxApplicationSubmissions = yarnClusterJobSubmissionParameters.getMaxApplicationSubmissions();
    this.driverRecoveryTimeout = this.yarnAppSubmissionParameters.getDriverRecoveryTimeout();
    this.driverMemory = yarnClusterJobSubmissionParameters.getDriverMemory();
    this.priority = DEFAULT_PRIORITY;
    this.queue = DEFAULT_QUEUE;
    this.tokenKind = yarnClusterJobSubmissionParameters.getSecurityTokenKind().toString();
    this.tokenService = yarnClusterJobSubmissionParameters.getSecurityTokenService().toString();
    this.fileSystemUrl = yarnJobSubmissionParameters.getFileSystemUrl().toString();
    this.jobSubmissionDirectoryPrefix = yarnJobSubmissionParameters.getJobSubmissionDirectoryPrefix().toString();
    this.yarnDriverStdoutFilePath = yarnClusterJobSubmissionParameters.getDriverStdoutFilePath().toString();
    this.yarnDriverStderrFilePath = yarnClusterJobSubmissionParameters.getDriverStderrFilePath().toString();

    if (yarnClusterJobSubmissionParameters.getEnvironmentVariablesMap() != null) {
      for (Map.Entry<java.lang.CharSequence, java.lang.CharSequence> pair :
          yarnClusterJobSubmissionParameters.getEnvironmentVariablesMap().entrySet()) {
        this.environmentVariablesMap.put(pair.getKey().toString(), pair.getValue().toString());
      }
    }

    Validate.notEmpty(jobId, "The job id is null or empty");
    Validate.isTrue(driverMemory > 0, "The amount of driver memory given is <= 0.");
    Validate.isTrue(tcpBeginPort >= 0, "The tcp start port given is < 0.");
    Validate.isTrue(tcpRangeCount > 0, "The tcp range given is <= 0.");
    Validate.isTrue(tcpTryCount > 0, "The tcp retry count given is <= 0.");
    Validate.isTrue(maxApplicationSubmissions > 0, "The maximum number of app submissions given is <= 0.");
    Validate.notEmpty(queue, "The queue is null or empty");
    Validate.notEmpty(tokenKind, "Token kind should be either NULL or some custom non empty value");
    Validate.notEmpty(tokenService, "Token service should be either NULL or some custom non empty value");
    Validate.notEmpty(fileSystemUrl, "File system Url should be either NULL or some custom non empty value");
    Validate.notEmpty(jobSubmissionDirectoryPrefix, "Job submission directory prefix should not be empty");
    Validate.notEmpty(yarnDriverStdoutFilePath, "Driver stdout file path should not be empty");
    Validate.notEmpty(yarnDriverStderrFilePath, "Driver stderr file path should not be empty");
  }

  @Override
  public String toString() {
    return "YarnClusterSubmissionFromCS{" +
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
        ", fileSystemUrl='" + fileSystemUrl + '\'' +
        ", jobSubmissionDirectoryPrefix='" + jobSubmissionDirectoryPrefix + '\'' +
        envMapString() +
        '}';
  }

  private String envMapString() {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, String> entry : environmentVariablesMap.entrySet()) {
      sb.append(", Key:" + entry.getKey() + ", value:" + entry.getValue());
    }
    return sb.toString();
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
   * @return The file system url
   */
  String getFileSystemUrl() {
    return fileSystemUrl;
  }

  /**
   * @return The environment map.
   */
  Map<String, String> getEnvironmentVariablesMap() {
    return new HashMap<>(environmentVariablesMap);
  }

  /**
   * @return The max amount of times the application can be submitted.
   */
  int getMaxApplicationSubmissions(){
    return maxApplicationSubmissions;
  }

  /**
   * @return The time allowed for Driver recovery to recover all its Evaluators.
   */
  int getDriverRecoveryTimeout() {
    return driverRecoveryTimeout;
  }

  String getYarnDriverStdoutFilePath() {
    return yarnDriverStdoutFilePath;
  }

  String getYarnDriverStderrFilePath() {
    return yarnDriverStderrFilePath;
  }

  /**
   * @return The submission parameters for YARN applications.
   */
  AvroYarnAppSubmissionParameters getYarnAppSubmissionParameters() {
    return yarnAppSubmissionParameters;
  }

  /**
   * @return The submission parameters for YARN jobs.
   */
  AvroYarnJobSubmissionParameters getYarnJobSubmissionParameters() {
    return yarnJobSubmissionParameters;
  }

  /**
   * @return job Submission Directory Prefix which is serialized from C#
   */
  String getJobSubmissionDirectoryPrefix() {
    return jobSubmissionDirectoryPrefix;
  }

  /**
   * Takes the YARN cluster job submission configuration file, deserializes it, and creates submission object.
   */
  static YarnClusterSubmissionFromCS fromJobSubmissionParametersFile(final File yarnClusterAppSubmissionParametersFile,
                                                                     final File yarnClusterJobSubmissionParametersFile)
      throws IOException {
    try (final FileInputStream appFileInputStream = new FileInputStream(yarnClusterAppSubmissionParametersFile)) {
      try (final FileInputStream jobFileInputStream = new FileInputStream(yarnClusterJobSubmissionParametersFile)) {
        // this is mainly a test hook
        return readYarnClusterSubmissionFromCSFromInputStream(appFileInputStream, jobFileInputStream);
      }
    }
  }

  static YarnClusterSubmissionFromCS readYarnClusterSubmissionFromCSFromInputStream(
      final InputStream appInputStream, final InputStream jobInputStream) throws IOException {
    final JsonDecoder appDecoder = DecoderFactory.get().jsonDecoder(
        AvroYarnAppSubmissionParameters.getClassSchema(), appInputStream);
    final SpecificDatumReader<AvroYarnAppSubmissionParameters> appReader = new SpecificDatumReader<>(
        AvroYarnAppSubmissionParameters.class);
    final AvroYarnAppSubmissionParameters yarnClusterAppSubmissionParameters = appReader.read(null, appDecoder);

    final JsonDecoder jobDecoder = DecoderFactory.get().jsonDecoder(
        AvroYarnClusterJobSubmissionParameters.getClassSchema(), jobInputStream);
    final SpecificDatumReader<AvroYarnClusterJobSubmissionParameters> jobReader = new SpecificDatumReader<>(
        AvroYarnClusterJobSubmissionParameters.class);
    final AvroYarnClusterJobSubmissionParameters yarnClusterJobSubmissionParameters = jobReader.read(null, jobDecoder);

    return new YarnClusterSubmissionFromCS(yarnClusterAppSubmissionParameters, yarnClusterJobSubmissionParameters);
  }
}
