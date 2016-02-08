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
package org.apache.reef.runtime.hdinsight.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.hdinsight.client.yarnrest.*;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles job submission to a HDInsight instance.
 */
@ClientSide
@Private
public final class HDInsightJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(HDInsightJobSubmissionHandler.class.getName());

  private final AzureUploader uploader;
  private final JobJarMaker jobJarMaker;
  private final HDInsightInstance hdInsightInstance;
  private final REEFFileNames filenames;
  private final ClasspathProvider classpath;
  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  HDInsightJobSubmissionHandler(final AzureUploader uploader,
                                final JobJarMaker jobJarMaker,
                                final HDInsightInstance hdInsightInstance,
                                final REEFFileNames filenames,
                                final ClasspathProvider classpath,
                                final DriverConfigurationProvider driverConfigurationProvider) {
    this.uploader = uploader;
    this.jobJarMaker = jobJarMaker;
    this.hdInsightInstance = hdInsightInstance;
    this.filenames = filenames;
    this.classpath = classpath;
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  @Override
  public void close() {
    LOG.log(Level.WARNING, ".close() is inconsequential with the HDInsight runtime");
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    try {

      LOG.log(Level.FINE, "Requesting Application ID from HDInsight.");
      final ApplicationID applicationID = this.hdInsightInstance.getApplicationID();

      LOG.log(Level.INFO, "Submitting application {0} to YARN.", applicationID.getApplicationId());

      LOG.log(Level.FINE, "Creating a job folder on Azure.");
      final String jobFolderURL = this.uploader.createJobFolder(applicationID.getApplicationId());

      LOG.log(Level.FINE, "Assembling Configuration for the Driver.");
      final Configuration driverConfiguration =
          makeDriverConfiguration(jobSubmissionEvent, applicationID.getApplicationId(), jobFolderURL);

      LOG.log(Level.FINE, "Making Job JAR.");
      final File jobSubmissionJarFile =
          this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);

      LOG.log(Level.FINE, "Uploading Job JAR to Azure.");
      final LocalResource uploadedFile = this.uploader.uploadFile(jobSubmissionJarFile);

      LOG.log(Level.FINE, "Assembling application submission.");
      final String command = getCommandString(jobSubmissionEvent);

      final ApplicationSubmission applicationSubmission = new ApplicationSubmission()
          .setApplicationId(applicationID.getApplicationId())
          .setApplicationName(jobSubmissionEvent.getIdentifier())
          .setResource(getResource(jobSubmissionEvent))
          .setAmContainerSpec(new AmContainerSpec()
                  .addLocalResource(this.filenames.getREEFFolderName(), uploadedFile)
                  .setCommand(command));

      this.hdInsightInstance.submitApplication(applicationSubmission);
      LOG.log(Level.INFO, "Submitted application to HDInsight. The application id is: {0}",
          applicationID.getApplicationId());

    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Error submitting HDInsight request", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Extracts the resource demands from the jobSubmissionEvent.
   */
  private Resource getResource(
      final JobSubmissionEvent jobSubmissionEvent) {

    return new Resource()
        .setMemory(jobSubmissionEvent.getDriverMemory().get())
        .setvCores(1);
  }

  /**
   * Assembles the command to execute the Driver.
   */
  private String getCommandString(
      final JobSubmissionEvent jobSubmissionEvent) {
    return StringUtils.join(getCommandList(jobSubmissionEvent), ' ');
  }

  /**
   * Assembles the command to execute the Driver in list form.
   */
  private List<String> getCommandList(
      final JobSubmissionEvent jobSubmissionEvent) {

    return new JavaLaunchCommandBuilder()
        .setJavaPath("%JAVA_HOME%/bin/java")
        .setConfigurationFilePaths(Collections.singletonList(this.filenames.getDriverConfigurationPath()))
        .setClassPath(this.classpath.getDriverClasspath())
        .setMemory(jobSubmissionEvent.getDriverMemory().get())
        .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.filenames.getDriverStderrFileName())
        .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.filenames.getDriverStdoutFileName())
        .build();
  }

  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final String applicationId,
      final String jobFolderURL) throws IOException {

    return this.driverConfigurationProvider.getDriverConfiguration(jobFolderURL,
                                                            jobSubmissionEvent.getRemoteId(),
                                                            applicationId,
                                                            jobSubmissionEvent.getConfiguration());
  }
}
