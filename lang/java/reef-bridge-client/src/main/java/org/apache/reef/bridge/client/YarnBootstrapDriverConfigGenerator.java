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
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.javabridge.generic.JobDriver;
import org.apache.reef.reef.bridge.client.avro.AvroAppSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroJobSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroYarnAppSubmissionParameters;
import org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.runtime.yarn.driver.YarnDriverRestartConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.tang.*;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the Java Driver configuration generator for .NET Drivers that generates
 * the Driver configuration at runtime. Called by {@link YarnBootstrapREEFLauncher}.
 */
final class YarnBootstrapDriverConfigGenerator {
  private static final Logger LOG = Logger.getLogger(YarnBootstrapDriverConfigGenerator.class.getName());

  private final REEFFileNames reefFileNames;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private YarnBootstrapDriverConfigGenerator(final REEFFileNames reefFileNames,
                                             final ConfigurationSerializer configurationSerializer) {
    this.configurationSerializer = configurationSerializer;
    this.reefFileNames = reefFileNames;
  }

  public String writeDriverConfigurationFile(final String bootstrapJobArgsLocation,
                                             final String bootstrapAppArgsLocation) throws IOException {
    final File bootstrapJobArgsFile = new File(bootstrapJobArgsLocation).getCanonicalFile();
    final File bootstrapAppArgsFile = new File(bootstrapAppArgsLocation);

    final AvroYarnJobSubmissionParameters yarnBootstrapJobArgs =
        readYarnJobSubmissionParametersFromFile(bootstrapJobArgsFile);

    final AvroYarnAppSubmissionParameters yarnBootstrapAppArgs =
        readYarnAppSubmissionParametersFromFile(bootstrapAppArgsFile);

    final String driverConfigPath = reefFileNames.getDriverConfigurationPath();

    this.configurationSerializer.toFile(getYarnDriverConfiguration(yarnBootstrapJobArgs, yarnBootstrapAppArgs),
        new File(driverConfigPath));

    return driverConfigPath;
  }

  static Configuration getYarnDriverConfiguration(
      final AvroYarnJobSubmissionParameters yarnJobSubmissionParams,
      final AvroYarnAppSubmissionParameters yarnAppSubmissionParams) {

    final AvroJobSubmissionParameters jobSubmissionParameters =
        yarnJobSubmissionParams.getSharedJobSubmissionParameters();
    final Configuration yarnDriverConfiguration = YarnDriverConfiguration.CONF
        .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY,
            yarnJobSubmissionParams.getDfsJobSubmissionFolder().toString())
        .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobSubmissionParameters.getJobId().toString())
        .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
        .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
        .build();

    final AvroAppSubmissionParameters appSubmissionParams = yarnAppSubmissionParams.getSharedAppSubmissionParameters();

    final Configuration providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(appSubmissionParams.getTcpBeginPort()))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(appSubmissionParams.getTcpRangeCount()))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(appSubmissionParams.getTcpTryCount()))
        .bindNamedParameter(JobSubmissionDirectoryPrefix.class,
            yarnJobSubmissionParams.getJobSubmissionDirectoryPrefix().toString())
        .build();

    final Configuration driverConfiguration = Configurations.merge(
        Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER, yarnDriverConfiguration, providerConfig);

    if (yarnAppSubmissionParams.getDriverRecoveryTimeout() > 0) {
      LOG.log(Level.FINE, "Driver restart is enabled.");

      final Configuration yarnDriverRestartConfiguration =
          YarnDriverRestartConfiguration.CONF.build();

      final Configuration driverRestartConfiguration =
          DriverRestartConfiguration.CONF
              .set(DriverRestartConfiguration.ON_DRIVER_RESTARTED, JobDriver.RestartHandler.class)
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE,
                  JobDriver.DriverRestartActiveContextHandler.class)
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_TASK_RUNNING,
                  JobDriver.DriverRestartRunningTaskHandler.class)
              .set(DriverRestartConfiguration.DRIVER_RESTART_EVALUATOR_RECOVERY_SECONDS,
                  yarnAppSubmissionParams.getDriverRecoveryTimeout())
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
                  JobDriver.DriverRestartCompletedHandler.class)
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
                  JobDriver.DriverRestartFailedEvaluatorHandler.class)
              .build();

      return Configurations.merge(driverConfiguration, yarnDriverRestartConfiguration, driverRestartConfiguration);
    }

    return driverConfiguration;
  }

  static AvroYarnAppSubmissionParameters readYarnAppSubmissionParametersFromFile(final File file)
      throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(file)) {
      // This is mainly a test hook.
      return readYarnAppSubmissionParametersFromInputStream(fileInputStream);
    }
  }

  static AvroYarnAppSubmissionParameters readYarnAppSubmissionParametersFromInputStream(
      final InputStream inputStream) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
        AvroYarnAppSubmissionParameters.getClassSchema(), inputStream);
    final SpecificDatumReader<AvroYarnAppSubmissionParameters> reader = new SpecificDatumReader<>(
        AvroYarnAppSubmissionParameters.class);
    return reader.read(null, decoder);
  }

  static AvroYarnJobSubmissionParameters readYarnJobSubmissionParametersFromFile(final File file)
      throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(file)) {
      // This is mainly a test hook.
      return readYarnJobSubmissionParametersFromInputStream(fileInputStream);
    }
  }

  static AvroYarnJobSubmissionParameters readYarnJobSubmissionParametersFromInputStream(
      final InputStream inputStream) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
        AvroYarnJobSubmissionParameters.getClassSchema(), inputStream);
    final SpecificDatumReader<AvroYarnJobSubmissionParameters> reader = new SpecificDatumReader<>(
        AvroYarnJobSubmissionParameters.class);
    return reader.read(null, decoder);
  }
}