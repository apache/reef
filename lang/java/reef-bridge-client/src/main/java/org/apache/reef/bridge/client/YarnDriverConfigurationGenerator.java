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

import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.javabridge.generic.JobDriver;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.driver.JobSubmissionDirectoryProvider;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.runtime.yarn.driver.YarnDriverRestartConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Does client side manipulation of driver configuration for YARN runtime.
 */
final class YarnDriverConfigurationGenerator {
  private static final Logger LOG = Logger.getLogger(YarnDriverConfigurationGenerator.class.getName());
  private final Set<ConfigurationProvider> configurationProviders;
  private final int driverRestartEvaluatorRecoverySeconds;
  private final REEFFileNames fileNames;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private YarnDriverConfigurationGenerator(@Parameter(DriverConfigurationProviders.class)
                                   final Set<ConfigurationProvider> configurationProviders,
                                   @Parameter(SubmissionDriverRestartEvaluatorRecoverySeconds.class)
                                   final int driverRestartEvaluatorRecoverySeconds,
                                   final ConfigurationSerializer configurationSerializer,
                                   final REEFFileNames fileNames) {
    this.configurationProviders = configurationProviders;
    this.driverRestartEvaluatorRecoverySeconds = driverRestartEvaluatorRecoverySeconds;
    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;
  }

  /**
   * Writes driver configuration to disk.
   * @param driverFolder the folder containing the `reef` folder. Only that `reef` folder will be in the JAR.
   * @param jobId id of the job to be submitted
   * @param jobSubmissionFolder job submission folder on DFS
   * @return the prepared driver configuration
   * @throws IOException
   */
  public Configuration writeConfiguration(final File driverFolder,
                                          final String jobId,
                                          final String jobSubmissionFolder) throws IOException {
    final File driverConfigurationFile = new File(driverFolder, this.fileNames.getDriverConfigurationPath());
    final Configuration yarnDriverConfiguration = YarnDriverConfiguration.CONF
        .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobSubmissionFolder)
        .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobId)
        .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
        .set(YarnDriverConfiguration.JVM_HEAP_SLACK, 0.0)
        .build();

    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final ConfigurationProvider configurationProvider : this.configurationProviders) {
      configurationBuilder.addConfiguration(configurationProvider.getConfiguration());
    }
    final Configuration providedConfigurations =  configurationBuilder.build();

    Configuration driverConfiguration = Configurations.merge(
        Constants.DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER,
        yarnDriverConfiguration,
        providedConfigurations);

    if (driverRestartEvaluatorRecoverySeconds > 0) {
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
            driverRestartEvaluatorRecoverySeconds)
          .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
            JobDriver.DriverRestartCompletedHandler.class)
          .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
            JobDriver.DriverRestartFailedEvaluatorHandler.class)
          .build();

      driverConfiguration = Configurations.merge(
        driverConfiguration, yarnDriverRestartConfiguration, driverRestartConfiguration);
    }

    this.configurationSerializer.toFile(driverConfiguration, driverConfigurationFile);
    return driverConfiguration;
  }

  /**
   * This main is executed from .NET to perform driver config generation.
   * For arguments detail:
   * @see org.apache.reef.bridge.client.YarnSubmissionFromCS#fromJobSubmissionParametersFile(File)
   */
  public static void main(final String[] args) throws InjectionException, IOException {
    final File jobSubmissionParametersFile = new File(args[0]);
    if (!(jobSubmissionParametersFile.exists() && jobSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + jobSubmissionParametersFile.getAbsolutePath());
    }

    final YarnSubmissionFromCS yarnSubmission =
        YarnSubmissionFromCS.fromJobSubmissionParametersFile(jobSubmissionParametersFile);

    LOG.log(Level.INFO, "YARN driver config generation received from C#: {0}", yarnSubmission);
    final Configuration yarnConfiguration = yarnSubmission.getRuntimeConfiguration();

    final Injector injector = Tang.Factory.getTang().newInjector(yarnConfiguration);
    final YarnDriverConfigurationGenerator yarnClientConfigurationGenerator =
        injector.getInstance(YarnDriverConfigurationGenerator.class);
    final JobSubmissionDirectoryProvider directoryProvider = injector.getInstance(JobSubmissionDirectoryProvider.class);
    final String jobId = yarnSubmission.getJobId();

    LOG.log(Level.INFO, "Writing driver config for job {0}", jobId);
    yarnClientConfigurationGenerator.writeConfiguration(
        yarnSubmission.getDriverFolder(), jobId, directoryProvider.getJobSubmissionDirectoryPath(jobId).toString());
    System.exit(0);
    LOG.log(Level.INFO, "End of main in Java YarnDriverConfigurationGenerator");
  }
}

/**
 * How long the driver should wait before timing out on evaluator
 * recovery in seconds. Defaults to -1. If value is negative, the restart functionality will not be
 * enabled. Only used by .NET job submission.
 */
@NamedParameter(doc = "How long the driver should wait before timing out on evaluator" +
    " recovery in seconds. Defaults to -1. If value is negative, the restart functionality will not be" +
    " enabled. Only used by .NET job submission.", default_value = "-1")
final class SubmissionDriverRestartEvaluatorRecoverySeconds implements Name<Integer> {
  private SubmissionDriverRestartEvaluatorRecoverySeconds() {
  }
}
