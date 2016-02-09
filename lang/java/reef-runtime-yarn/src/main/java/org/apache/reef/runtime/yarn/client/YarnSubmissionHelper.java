/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.runtime.common.REEFLauncher;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.yarn.util.YarnTypes;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper code that wraps the YARN Client API for our purposes.
 */
public final class YarnSubmissionHelper implements Closeable{
  private static final Logger LOG = Logger.getLogger(YarnSubmissionHelper.class.getName());

  private final YarnClient yarnClient;
  private final YarnClientApplication yarnClientApplication;
  private final GetNewApplicationResponse applicationResponse;
  private final ApplicationSubmissionContext applicationSubmissionContext;
  private final ApplicationId applicationId;
  private final Map<String, LocalResource> resources = new HashMap<>();
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final SecurityTokenProvider tokenProvider;
  private final List<String> commandPrefixList;
  private Class launcherClazz;
  private List<String> configurationFilePaths;

  public YarnSubmissionHelper(final YarnConfiguration yarnConfiguration,
                              final REEFFileNames fileNames,
                              final ClasspathProvider classpath,
                              final SecurityTokenProvider tokenProvider,
                              final List<String> commandPrefixList) throws IOException, YarnException {
    this.fileNames = fileNames;
    this.classpath = classpath;

    LOG.log(Level.FINE, "Initializing YARN Client");
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(yarnConfiguration);
    this.yarnClient.start();
    LOG.log(Level.FINE, "Initialized YARN Client");

    LOG.log(Level.FINE, "Requesting Application ID from YARN.");
    this.yarnClientApplication = this.yarnClient.createApplication();
    this.applicationResponse = yarnClientApplication.getNewApplicationResponse();
    this.applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
    this.applicationId = applicationSubmissionContext.getApplicationId();
    this.tokenProvider = tokenProvider;
    this.commandPrefixList = commandPrefixList;
    this.launcherClazz = REEFLauncher.class;
    this.configurationFilePaths = Collections.singletonList(this.fileNames.getDriverConfigurationPath());
    LOG.log(Level.FINEST, "YARN Application ID: {0}", applicationId);
  }

  public YarnSubmissionHelper(final YarnConfiguration yarnConfiguration,
                              final REEFFileNames fileNames,
                              final ClasspathProvider classpath,
                              final SecurityTokenProvider tokenProvider) throws IOException, YarnException {
    this(yarnConfiguration, fileNames, classpath, tokenProvider, null);
  }

  /**
   *
   * @return the application ID assigned by YARN.
   */
  public int getApplicationId() {
    return this.applicationId.getId();
  }

  /**
   *
   * @return the application ID string representation assigned by YARN.
   */
  public String getStringApplicationId() {
    return this.applicationId.toString();
  }

  /**
   * Set the name of the application to be submitted.
   * @param applicationName
   * @return
   */
  public YarnSubmissionHelper setApplicationName(final String applicationName) {
    applicationSubmissionContext.setApplicationName(applicationName);
    return this;
  }

  /**
   * Set the amount of memory to be allocated to the Driver.
   * @param megabytes
   * @return
   */
  public YarnSubmissionHelper setDriverMemory(final int megabytes) {
    applicationSubmissionContext.setResource(Resource.newInstance(getMemory(megabytes), 1));
    return this;
  }

  /**
   * Add a file to be localized on the driver.
   * @param resourceName
   * @param resource
   * @return
   */
  public YarnSubmissionHelper addLocalResource(final String resourceName, final LocalResource resource) {
    resources.put(resourceName, resource);
    return this;
  }

  /**
   * Set the priority of the job.
   * @param priority
   * @return
   */
  public YarnSubmissionHelper setPriority(final int priority) {
    this.applicationSubmissionContext.setPriority(Priority.newInstance(priority));
    return this;
  }

  /**
   * Set whether or not the resource manager should preserve evaluators across driver restarts.
   * @param preserveEvaluators
   * @return
   */
  public YarnSubmissionHelper setPreserveEvaluators(final boolean preserveEvaluators) {
    if (preserveEvaluators) {
      // when supported, set KeepContainersAcrossApplicationAttempts to be true
      // so that when driver (AM) crashes, evaluators will still be running and we can recover later.
      if (YarnTypes.isAtOrAfterVersion(YarnTypes.MIN_VERSION_KEEP_CONTAINERS_AVAILABLE)) {
        LOG.log(
            Level.FINE,
            "Hadoop version is {0} or after with KeepContainersAcrossApplicationAttempts supported," +
                " will set it to true.",
            YarnTypes.MIN_VERSION_KEEP_CONTAINERS_AVAILABLE);

        applicationSubmissionContext.setKeepContainersAcrossApplicationAttempts(true);
      } else {
        LOG.log(Level.WARNING,
            "Hadoop version does not yet support KeepContainersAcrossApplicationAttempts. Driver restarts " +
                "will not support recovering evaluators.");

        applicationSubmissionContext.setKeepContainersAcrossApplicationAttempts(false);
      }
    } else {
      applicationSubmissionContext.setKeepContainersAcrossApplicationAttempts(false);
    }

    return this;
  }

  /**
   * Sets the maximum application attempts for the application.
   * @param maxApplicationAttempts
   * @return
   */
  public YarnSubmissionHelper setMaxApplicationAttempts(final int maxApplicationAttempts) {
    applicationSubmissionContext.setMaxAppAttempts(maxApplicationAttempts);
    return this;
  }

  /**
   * Assign this job submission to a queue.
   * @param queueName
   * @return
   */
  public YarnSubmissionHelper setQueue(final String queueName) {
    this.applicationSubmissionContext.setQueue(queueName);
    return this;
  }

  /**
   * Sets the launcher class for the job.
   * @param launcherClass
   * @return
   */
  public YarnSubmissionHelper setLauncherClass(final Class launcherClass) {
    this.launcherClazz = launcherClass;
    return this;
  }

  /**
   * Sets the configuration file for the job.
   * Note that this does not have to be Driver TANG configuration. In the bootstrap
   * launch case, this can be the set of  the Avro files that supports the generation of a driver
   * configuration file natively at the Launcher.
   * @param configurationFilePaths
   * @return
   */
  public YarnSubmissionHelper setConfigurationFilePaths(final List<String> configurationFilePaths) {
    this.configurationFilePaths = configurationFilePaths;
    return this;
  }

  public void submit() throws IOException, YarnException {

    // SET EXEC COMMAND
    final List<String> launchCommand = new JavaLaunchCommandBuilder(launcherClazz, commandPrefixList)
        .setConfigurationFilePaths(configurationFilePaths)
        .setClassPath(this.classpath.getDriverClasspath())
        .setMemory(this.applicationSubmissionContext.getResource().getMemory())
        .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.fileNames.getDriverStdoutFileName())
        .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.fileNames.getDriverStderrFileName())
        .build();

    if (this.applicationSubmissionContext.getKeepContainersAcrossApplicationAttempts() &&
        this.applicationSubmissionContext.getMaxAppAttempts() == 1) {
      LOG.log(Level.WARNING, "Application will not be restarted even though preserve evaluators is set to true" +
          " since the max application submissions is 1. Proceeding to submit application...");
    }

    final ContainerLaunchContext containerLaunchContext = YarnTypes.getContainerLaunchContext(
        launchCommand, this.resources, tokenProvider.getTokens());
    this.applicationSubmissionContext.setAMContainerSpec(containerLaunchContext);

    LOG.log(Level.INFO, "Submitting REEF Application to YARN. ID: {0}", this.applicationId);

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "REEF app command: {0}", StringUtils.join(launchCommand, ' '));
    }

    this.yarnClient.submitApplication(applicationSubmissionContext);
  }

  /**
   * Extract the desired driver memory from jobSubmissionProto.
   * <p>
   * returns maxMemory if that desired amount is more than maxMemory
   */
  private int getMemory(final int requestedMemory) {
    final int maxMemory = applicationResponse.getMaximumResourceCapability().getMemory();
    final int amMemory;

    if (requestedMemory <= maxMemory) {
      amMemory = requestedMemory;
    } else {
      LOG.log(Level.WARNING,
          "Requested {0}MB of memory for the driver. " +
              "The max on this YARN installation is {1}. " +
              "Using {1} as the memory for the driver.",
          new Object[]{requestedMemory, maxMemory});
      amMemory = maxMemory;
    }
    return amMemory;
  }

  @Override
  public void close() throws IOException {
    this.yarnClient.stop();
  }
}
