/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.yarn.util.YarnTypes;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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


  public YarnSubmissionHelper(final YarnConfiguration yarnConfiguration,
                              final REEFFileNames fileNames,
                              final ClasspathProvider classpath) throws IOException, YarnException {
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
    LOG.log(Level.FINEST, "YARN Application ID: {0}", applicationId);
  }

  /**
   *
   * @return the application ID assigned by YARN.
   */
  public int getApplicationId() {
    return this.applicationId.getId();
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
   * Assign this job submission to a queue.
   * @param queueName
   * @return
   */
  public YarnSubmissionHelper setQueue(final String queueName) {
    this.applicationSubmissionContext.setQueue(queueName);
    return this;
  }

  public void submit(final String clientRemoteIdentifier) throws IOException, YarnException {
    // SET EXEC COMMAND
    final List<String> launchCommand = new JavaLaunchCommandBuilder()
        .setErrorHandlerRID(clientRemoteIdentifier)
        .setLaunchID(this.applicationSubmissionContext.getApplicationName())
        .setConfigurationFileName(this.fileNames.getDriverConfigurationPath())
        .setClassPath(this.classpath.getDriverClasspath())
        .setMemory(this.applicationSubmissionContext.getResource().getMemory())
        .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.fileNames.getDriverStdoutFileName())
        .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.fileNames.getDriverStderrFileName())
        .build();

    this.applicationSubmissionContext.setAMContainerSpec(YarnTypes.getContainerLaunchContext(launchCommand,
        this.resources));

    LOG.log(Level.INFO, "Submitting REEF Application to YARN. ID: {0}", this.applicationId);

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "REEF app command: {0}", StringUtils.join(launchCommand, ' '));
    }

    // TODO: this is currently being developed on a hacked 2.4.0 bits, should be 2.4.1
    final String minVersionKeepContainerOptionAvailable = "2.4.0";

    // when supported, set KeepContainersAcrossApplicationAttempts to be true
    // so that when driver (AM) crashes, evaluators will still be running and we can recover later.
    if (YarnTypes.isAtOrAfterVersion(minVersionKeepContainerOptionAvailable)) {
      LOG.log(
          Level.FINE,
          "Hadoop version is {0} or after with KeepContainersAcrossApplicationAttempts supported, will set it to true.",
          minVersionKeepContainerOptionAvailable);

      applicationSubmissionContext.setKeepContainersAcrossApplicationAttempts(true);
    }

    this.yarnClient.submitApplication(applicationSubmissionContext);
  }

  /**
   * Extract the desired driver memory from jobSubmissionProto.
   * <p/>
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
