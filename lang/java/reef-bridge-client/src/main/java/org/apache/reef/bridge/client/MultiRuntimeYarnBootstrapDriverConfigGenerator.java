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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.javabridge.generic.JobDriver;
import org.apache.reef.reef.bridge.client.avro.*;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.multi.client.*;
import org.apache.reef.runtime.multi.driver.MultiRuntimeDriverConfiguration;
import org.apache.reef.runtime.multi.utils.MultiRuntimeDefinitionSerializer;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.driver.*;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeTryCount;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the Java Driver configuration generator for .NET Drivers that generates
 * the Driver configuration at runtime for multiruntime. Called by {@link MultiRuntimeYarnBootstrapREEFLauncher}.
 */
final class MultiRuntimeYarnBootstrapDriverConfigGenerator {
  private static final Logger LOG = Logger.getLogger(MultiRuntimeYarnBootstrapDriverConfigGenerator.class.getName());
  private static final String DUMMY_YARN_RUNTIME = "DummyYarnRuntime";

  private final MultiRuntimeDefinitionSerializer runtimeDefinitionSerializer = new MultiRuntimeDefinitionSerializer();

  private final REEFFileNames reefFileNames;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private MultiRuntimeYarnBootstrapDriverConfigGenerator(final REEFFileNames reefFileNames,
                                                         final ConfigurationSerializer configurationSerializer) {
    this.configurationSerializer = configurationSerializer;
    this.reefFileNames = reefFileNames;
  }

  /**
   * Adds yarn runtime definitions to the builder.
   * @param yarnJobSubmissionParams Yarn job submission parameters
   * @param jobSubmissionParameters Generic job submission parameters
   * @param builder The multi runtime builder
   */
  private void addYarnRuntimeDefinition(
          final AvroYarnJobSubmissionParameters yarnJobSubmissionParams,
          final AvroJobSubmissionParameters jobSubmissionParameters,
          final MultiRuntimeDefinitionBuilder builder) {
    // create and serialize yarn configuration if defined
    final Configuration yarnDriverConfiguration =
            createYarnConfiguration(yarnJobSubmissionParams, jobSubmissionParameters);

    // add yarn runtime to the builder
    builder.addRuntime(yarnDriverConfiguration, RuntimeIdentifier.RUNTIME_NAME);
  }

  private Configuration createYarnConfiguration(
          final AvroYarnJobSubmissionParameters yarnJobSubmissionParams,
          final AvroJobSubmissionParameters jobSubmissionParameters) {
    return YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY,
                    yarnJobSubmissionParams.getDfsJobSubmissionFolder().toString())
            .set(YarnDriverConfiguration.JOB_IDENTIFIER,
                    jobSubmissionParameters.getJobId().toString())
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER,
                    ClientRemoteIdentifier.NONE)
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
            .set(YarnDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
            .build();
  }

  /**
   * Adds yarn runtime definitions to the builder, with a dumy name, to prevent evaluator submission.
   * @param yarnJobSubmissionParams Yarn job submission parameters
   * @param jobSubmissionParameters Generic job submission parameters
   * @param builder The multi runtime builder
   */
  private void addDummyYarnRuntimeDefinition(
          final AvroYarnJobSubmissionParameters yarnJobSubmissionParams,
          final AvroJobSubmissionParameters jobSubmissionParameters,
          final MultiRuntimeDefinitionBuilder builder) {
    // create and serialize yarn configuration if defined
    final Configuration yarnDriverConfiguration =
            createYarnConfiguration(yarnJobSubmissionParams, jobSubmissionParameters);
    // add yarn runtime to the builder
    builder.addRuntime(yarnDriverConfiguration, DUMMY_YARN_RUNTIME);
  }
  /**
   * Adds local runtime definitions to the builder.
   * @param localAppSubmissionParams Local app submission parameters
   * @param jobSubmissionParameters Generic job submission parameters
   * @param builder The multi runtime builder
   */
  private void addLocalRuntimeDefinition(
          final AvroLocalAppSubmissionParameters localAppSubmissionParams,
          final AvroJobSubmissionParameters jobSubmissionParameters,
          final MultiRuntimeDefinitionBuilder builder) {
    // create and serialize local configuration if defined
    final Configuration localModule = LocalDriverConfiguration.CONF
            .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS,
                    localAppSubmissionParams.getMaxNumberOfConcurrentEvaluators())
            // ROOT FOLDER will point to the current runtime directory
            .set(LocalDriverConfiguration.ROOT_FOLDER, ".")
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 0.0)
            .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
            .set(LocalDriverConfiguration.JOB_IDENTIFIER,
                    jobSubmissionParameters.getJobId().toString())
            .set(LocalDriverConfiguration.RUNTIME_NAMES,
                    org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME)
            .build();

    // add local runtime to the builder
    builder.addRuntime(localModule, org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME);
  }

  private Configuration getMultiRuntimeDriverConfiguration(
          final AvroYarnJobSubmissionParameters yarnJobSubmissionParams,
          final AvroMultiRuntimeAppSubmissionParameters multiruntimeAppSubmissionParams) {

    if (multiruntimeAppSubmissionParams.getLocalRuntimeAppParameters() == null &&
            multiruntimeAppSubmissionParams.getYarnRuntimeAppParameters() == null){
      throw new IllegalArgumentException("At least on execution runtime has to be provided");
    }

    // read yarn job submission parameters
    final AvroJobSubmissionParameters jobSubmissionParameters =
            yarnJobSubmissionParams.getSharedJobSubmissionParameters();

    // generate multi runtime definition
    final MultiRuntimeDefinitionBuilder multiRuntimeDefinitionBuilder = new MultiRuntimeDefinitionBuilder();

    if (multiruntimeAppSubmissionParams.getLocalRuntimeAppParameters() != null){
      addLocalRuntimeDefinition(
              multiruntimeAppSubmissionParams.getLocalRuntimeAppParameters(),
              jobSubmissionParameters, multiRuntimeDefinitionBuilder);
    }

    if (multiruntimeAppSubmissionParams.getYarnRuntimeAppParameters() != null){
      addYarnRuntimeDefinition(
              yarnJobSubmissionParams,
              jobSubmissionParameters,
              multiRuntimeDefinitionBuilder);
    } else {
      addDummyYarnRuntimeDefinition(
              yarnJobSubmissionParams,
              jobSubmissionParameters,
              multiRuntimeDefinitionBuilder);
    }

    multiRuntimeDefinitionBuilder.setDefaultRuntimeName(
            multiruntimeAppSubmissionParams.getDefaultRuntimeName().toString());

    // generate multi runtime configuration
    ConfigurationModule multiRuntimeDriverConfiguration = MultiRuntimeDriverConfiguration.CONF
            .set(MultiRuntimeDriverConfiguration.JOB_IDENTIFIER, jobSubmissionParameters.getJobId().toString())
            .set(MultiRuntimeDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
            .set(MultiRuntimeDriverConfiguration.SERIALIZED_RUNTIME_DEFINITION,
                    this.runtimeDefinitionSerializer.toString(multiRuntimeDefinitionBuilder.build()));

    for (final CharSequence runtimeName : multiruntimeAppSubmissionParams.getRuntimes()){
      multiRuntimeDriverConfiguration = multiRuntimeDriverConfiguration.set(
              MultiRuntimeDriverConfiguration.RUNTIME_NAMES, runtimeName.toString());
    }

    final AvroAppSubmissionParameters appSubmissionParams =
            multiruntimeAppSubmissionParams.getSharedAppSubmissionParameters();

    // generate yarn related driver configuration
    final Configuration providerConfig = Tang.Factory.getTang().newConfigurationBuilder()
            .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class)
            .bindNamedParameter(TcpPortRangeBegin.class, Integer.toString(appSubmissionParams.getTcpBeginPort()))
            .bindNamedParameter(TcpPortRangeCount.class, Integer.toString(appSubmissionParams.getTcpRangeCount()))
            .bindNamedParameter(TcpPortRangeTryCount.class, Integer.toString(appSubmissionParams.getTcpTryCount()))
            .bindNamedParameter(JobSubmissionDirectoryPrefix.class,
                    yarnJobSubmissionParams.getJobSubmissionDirectoryPrefix().toString())
            .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
            .bindConstructor(YarnConfiguration.class, YarnConfigurationConstructor.class)
            .build();

    final Configuration driverConfiguration = Configurations.merge(
            Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER,
            multiRuntimeDriverConfiguration.build(),
            providerConfig);

    // add restart configuration if needed
    if (multiruntimeAppSubmissionParams.getYarnRuntimeAppParameters() != null &&
            multiruntimeAppSubmissionParams.getYarnRuntimeAppParameters().getDriverRecoveryTimeout() > 0) {
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
                              multiruntimeAppSubmissionParams.getYarnRuntimeAppParameters().getDriverRecoveryTimeout())
                      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
                              JobDriver.DriverRestartCompletedHandler.class)
                      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
                              JobDriver.DriverRestartFailedEvaluatorHandler.class)
                      .build();

      return Configurations.merge(driverConfiguration, yarnDriverRestartConfiguration, driverRestartConfiguration);
    }

    return driverConfiguration;
  }

  private static AvroMultiRuntimeAppSubmissionParameters readMultiRuntimeAppSubmissionParametersFromFile(
          final File file) throws IOException {
    try (final FileInputStream fileInputStream = new FileInputStream(file)) {
      // This is mainly a test hook.
      return readMultiRuntimeAppSubmissionParametersFromInputStream(fileInputStream);
    }
  }

  private static AvroYarnJobSubmissionParameters readYarnJobSubmissionParametersFromFile(final File file)
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

  static AvroMultiRuntimeAppSubmissionParameters readMultiRuntimeAppSubmissionParametersFromInputStream(
          final InputStream inputStream) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
            AvroMultiRuntimeAppSubmissionParameters.getClassSchema(), inputStream);
    final SpecificDatumReader<AvroMultiRuntimeAppSubmissionParameters> reader = new SpecificDatumReader<>(
            AvroMultiRuntimeAppSubmissionParameters.class);
    return reader.read(null, decoder);
  }

  /**
   * Writes the driver configuration files to the provided location.
   * @param bootstrapJobArgsLocation The path fro the job args file
   * @param bootstrapAppArgsLocation The path for the app args file
   * @throws IOException
   */
  public String writeDriverConfigurationFile(final String bootstrapJobArgsLocation,
                                             final String bootstrapAppArgsLocation) throws IOException {
    final File bootstrapJobArgsFile = new File(bootstrapJobArgsLocation).getCanonicalFile();
    final File bootstrapAppArgsFile = new File(bootstrapAppArgsLocation);

    final AvroYarnJobSubmissionParameters yarnBootstrapJobArgs =
            readYarnJobSubmissionParametersFromFile(bootstrapJobArgsFile);

    final AvroMultiRuntimeAppSubmissionParameters multiruntimeBootstrapAppArgs =
            readMultiRuntimeAppSubmissionParametersFromFile(bootstrapAppArgsFile);

    final String driverConfigPath = reefFileNames.getDriverConfigurationPath();

    this.configurationSerializer.toFile(
            getMultiRuntimeDriverConfiguration(
                    yarnBootstrapJobArgs, multiruntimeBootstrapAppArgs),
            new File(driverConfigPath));

    return driverConfigPath;
  }
}