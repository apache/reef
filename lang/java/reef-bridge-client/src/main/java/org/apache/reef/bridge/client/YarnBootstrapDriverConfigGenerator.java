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

import com.google.inject.Inject;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.javabridge.generic.JobDriver;
import org.apache.reef.reef.bridge.client.YarnBootstrapArgs;
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

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the Java Driver configuration generator for .NET Drivers that generates
 * the Driver configuration at runtime. This is used by the "yarn" flag of the
 * {@link BootstrapLauncher} to avoid Java dependencies for .NET clients during REST submission
 * to YARN clusters.
 * TODO[JIRA REEF-922]: Make sure that the generated Avro configurations work without dependencies on Java.
 */
public final class YarnBootstrapDriverConfigGenerator {
  private static final REEFFileNames REEF_FILE_NAMES = new REEFFileNames();
  private static final Logger LOG = Logger.getLogger(YarnBootstrapDriverConfigGenerator.class.getName());

  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private YarnBootstrapDriverConfigGenerator(final ConfigurationSerializer configurationSerializer) {
    this.configurationSerializer = configurationSerializer;
  }

  public String writeDriverConfiguration(final String bootstrapArgsLocation) throws IOException {
    final File bootstrapArgsFile = new File(bootstrapArgsLocation);
    final YarnBootstrapArgs yarnBootstrapArgs = getYarnBootstrapArgsFromFile(bootstrapArgsFile);
    final String driverConfigPath = REEF_FILE_NAMES.getDriverConfigurationPath();

    this.configurationSerializer.toFile(getYarnDriverConfiguration(yarnBootstrapArgs),
        new File(driverConfigPath));

    return driverConfigPath;
  }

  private static Configuration getYarnDriverConfiguration(final YarnBootstrapArgs yarnBootstrapArgs) {
    final Configuration providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
        .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(yarnBootstrapArgs.getTcpBeginPort()))
        .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(yarnBootstrapArgs.getTcpRangeCount()))
        .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(yarnBootstrapArgs.getTcpTryCount()))
        .bindNamedParameter(JobSubmissionDirectoryPrefix.class,
            yarnBootstrapArgs.getJobSubmissionDirectoryPrefix().toString())
        .build();

    final Configuration yarnDriverConfiguration = YarnDriverConfiguration.CONF
        .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, yarnBootstrapArgs.getJobSubmissionFolder().toString())
        .set(YarnDriverConfiguration.JOB_IDENTIFIER, yarnBootstrapArgs.getJobId().toString())
        .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
        .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
        .build();

    final Configuration driverConfiguration = Configurations.merge(
        Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER, yarnDriverConfiguration, providerConfig);

    if (yarnBootstrapArgs.getDriverRecoveryTimeout() > 0) {
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
                  yarnBootstrapArgs.getDriverRecoveryTimeout())
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
                  JobDriver.DriverRestartCompletedHandler.class)
              .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
                  JobDriver.DriverRestartFailedEvaluatorHandler.class)
              .build();

      return Configurations.merge(driverConfiguration, yarnDriverRestartConfiguration, driverRestartConfiguration);
    }

    return driverConfiguration;
  }

  private static YarnBootstrapArgs getYarnBootstrapArgsFromFile(final File file) throws IOException {
    final YarnBootstrapArgs yarnBootstrapArgs;
    try (final DataFileReader<YarnBootstrapArgs> dataFileReader =
             new DataFileReader<>(file, new SpecificDatumReader<>(YarnBootstrapArgs.class))) {
      yarnBootstrapArgs = dataFileReader.next();
    }
    return yarnBootstrapArgs;
  }
}
