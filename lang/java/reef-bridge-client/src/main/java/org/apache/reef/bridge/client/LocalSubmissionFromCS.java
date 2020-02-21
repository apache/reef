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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.reef.bridge.client.avro.AvroAppSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroLocalAppSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroLocalJobSubmissionParameters;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.address.LoopbackLocalAddressProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Represents a job submission from the CS code.
 * <p>
 * This class exists mostly to parse and validate the command line parameters provided by the C# class
 * `Org.Apache.REEF.Client.Local.LocalClient`
 */
final class LocalSubmissionFromCS {
  private final File driverFolder;
  private final File jobFolder;
  private final File runtimeRootFolder;
  private final String jobId;
  private final int maxNumberOfConcurrentEvaluators;
  private final int tcpBeginPort;
  private final int tcpRangeCount;
  private final int tcpTryCount;
  private final String driverStdoutPath;
  private final String driverStderrPath;

  private LocalSubmissionFromCS(final AvroLocalJobSubmissionParameters avroLocalJobSubmissionParameters,
                                final AvroLocalAppSubmissionParameters avroLocalAppSubmissionParameters) {
    // We assume the given path to be the one of the driver. The job folder is one level up from there.
    final AvroAppSubmissionParameters appSubmissionParameters =
        avroLocalAppSubmissionParameters.getSharedAppSubmissionParameters();
    this.tcpBeginPort = appSubmissionParameters.getTcpBeginPort();
    this.tcpRangeCount = appSubmissionParameters.getTcpRangeCount();
    this.tcpTryCount = appSubmissionParameters.getTcpTryCount();
    this.maxNumberOfConcurrentEvaluators = avroLocalAppSubmissionParameters.getMaxNumberOfConcurrentEvaluators();

    this.driverFolder = new File(avroLocalJobSubmissionParameters
        .getSharedJobSubmissionParameters().getJobSubmissionFolder().toString());

    this.jobId = avroLocalJobSubmissionParameters.getSharedJobSubmissionParameters().getJobId().toString();
    this.jobFolder = driverFolder.getParentFile();
    this.runtimeRootFolder = jobFolder.getParentFile();
    this.driverStdoutPath = avroLocalJobSubmissionParameters.getDriverStdoutFilePath().toString();
    this.driverStderrPath = avroLocalJobSubmissionParameters.getDriverStderrFilePath().toString();

    Validate.isTrue(driverFolder.exists(), "The driver folder does not exist.");
    Validate.notEmpty(jobId, "The job is is null or empty.");
    Validate.isTrue(maxNumberOfConcurrentEvaluators >= 0, "The number of evaluators is < 0.");
    Validate.isTrue(tcpBeginPort >= 0, "The tcp start port given is < 0.");
    Validate.isTrue(tcpRangeCount > 0, "The tcp range given is <= 0.");
    Validate.isTrue(tcpTryCount > 0, "The tcp retry count given is <= 0.");
    Validate.isTrue(StringUtils.isNotEmpty(driverStdoutPath), "The stdout path is empty.");
    Validate.isTrue(StringUtils.isNotEmpty(driverStderrPath), "The stderr path is empty.");
  }

  /**
   * @return the runtime configuration, based on the parameters passed from C#.
   */
  Configuration getRuntimeConfiguration() {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, Integer.toString(maxNumberOfConcurrentEvaluators))
        .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, runtimeRootFolder.getAbsolutePath())
        .build();

    final ArrayList<String> driverLaunchCommandPrefixList = new ArrayList<>();
    driverLaunchCommandPrefixList.add(
        new File(driverFolder,
            new REEFFileNames().getDriverLauncherExeFile().toString()
        ).toString());

    final Configuration userProviderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindSetEntry(DriverConfigurationProviders.class, LoopbackLocalAddressProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(tcpBeginPort))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(tcpRangeCount))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(tcpTryCount))
        .bindNamedParameter(JobSubmissionDirectory.class, runtimeRootFolder.getAbsolutePath())
        .bindList(DriverLaunchCommandPrefix.class, driverLaunchCommandPrefixList)
        .build();

    return Configurations.merge(runtimeConfiguration, userProviderConfiguration);
  }

  @Override
  public String toString() {
    return "LocalSubmissionFromCS{" +
        "driverFolder=" + driverFolder +
        ", jobFolder=" + jobFolder +
        ", runtimeRootFolder=" + runtimeRootFolder +
        ", jobId='" + jobId + '\'' +
        ", maxNumberOfConcurrentEvaluators=" + maxNumberOfConcurrentEvaluators +
        ", tcpBeginPort=" + tcpBeginPort +
        ", tcpRangeCount=" + tcpRangeCount +
        ", tcpTryCount=" + tcpTryCount +
        '}';
  }

  /**
   * @return The folder in which the job is staged.
   */
  File getJobFolder() {
    return jobFolder;
  }

  /**
   * @return The id of this job.
   */
  String getJobId() {
    return jobId;
  }

  /**
   * @return The Driver stdout path.
   */
  String getDriverStdoutPath() {
    return driverStdoutPath;
  }

  /**
   * @return The Driver stderr path.
   */
  String getDriverStderrPath() {
    return driverStderrPath;
  }

  /**
   * Takes the local job submission configuration file, deserializes it, and creates submission object.
   */
  static LocalSubmissionFromCS fromSubmissionParameterFiles(final File localJobSubmissionParametersFile,
                                                            final File localAppSubmissionParametersFile)
      throws IOException {
    final AvroLocalAppSubmissionParameters localAppSubmissionParameters;

    final AvroLocalJobSubmissionParameters localJobSubmissionParameters;

    try (FileInputStream fileInputStream = new FileInputStream(localJobSubmissionParametersFile)) {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          AvroLocalJobSubmissionParameters.getClassSchema(), fileInputStream);
      final SpecificDatumReader<AvroLocalJobSubmissionParameters> reader =
          new SpecificDatumReader<>(AvroLocalJobSubmissionParameters.class);
      localJobSubmissionParameters = reader.read(null, decoder);
    }

    try (FileInputStream fileInputStream = new FileInputStream(localAppSubmissionParametersFile)) {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          AvroLocalAppSubmissionParameters.getClassSchema(), fileInputStream);
      final SpecificDatumReader<AvroLocalAppSubmissionParameters> reader =
          new SpecificDatumReader<>(AvroLocalAppSubmissionParameters.class);
      localAppSubmissionParameters = reader.read(null, decoder);
    }

    return new LocalSubmissionFromCS(localJobSubmissionParameters, localAppSubmissionParameters);
  }
}
