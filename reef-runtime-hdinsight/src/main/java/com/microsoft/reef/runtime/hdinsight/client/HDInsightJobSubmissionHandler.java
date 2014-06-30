/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.files.JobJarMaker;
import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.hdinsight.client.yarnrest.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles job submission to a HDInsight instance.
 */
final class HDInsightJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(HDInsightJobSubmissionHandler.class.getName());

  private final AzureUploader uploader;
  private final JobJarMaker jobJarMaker;
  private final HDInsightInstance hdInsightInstance;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames filenames;

  @Inject
  HDInsightJobSubmissionHandler(final AzureUploader uploader,
                                final JobJarMaker jobJarMaker,
                                final HDInsightInstance hdInsightInstance,
                                final ConfigurationSerializer configurationSerializer,
                                final REEFFileNames filenames) {
    this.uploader = uploader;
    this.jobJarMaker = jobJarMaker;
    this.hdInsightInstance = hdInsightInstance;
    this.configurationSerializer = configurationSerializer;
    this.filenames = filenames;
  }

  @Override
  public void close() {
    LOG.log(Level.WARNING, ".close() is inconsequential with the HDInsight runtime");
  }

  @Override
  public void onNext(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {

    try {

      LOG.log(Level.INFO, "Requesting Application ID from YARN.");
      final ApplicationID applicationID = this.hdInsightInstance.getApplicationID();

      LOG.log(Level.INFO, "Creating a job folder on Azure.");
      final String jobFolderURL = this.uploader.createJobFolder(applicationID.getId());

      LOG.log(Level.INFO, "Assembling Configuration for the Driver.");
      final Configuration driverConfiguration =
          makeDriverConfiguration(jobSubmissionProto, applicationID.getId(), jobFolderURL);

      LOG.log(Level.INFO, "Making Job JAR.");
      final File jobSubmissionJarFile =
          this.jobJarMaker.createJobSubmissionJAR(jobSubmissionProto, driverConfiguration);

      LOG.log(Level.INFO, "Uploading Job JAR to Azure.");
      final FileResource uploadedFile = this.uploader.uploadFile(jobSubmissionJarFile);

      LOG.log(Level.INFO, "Assembling application submission.");
      final String command = getCommandString(jobSubmissionProto);
      final ApplicationSubmission applicationSubmission = new ApplicationSubmission()
          .setApplicationId(applicationID.getId())
          .setApplicationName(jobSubmissionProto.getIdentifier())
          .setResource(getResource(jobSubmissionProto))
          .setContainerInfo(new ContainerInfo()
                  .addFileResource(filenames.getREEFFolderName(), uploadedFile)
                  .addCommand(command)
                  .addEnvironment("CLASSPATH", getClassPath()));

      LOG.log(Level.INFO, "Submitting application {0} to YARN.", applicationID.getId());

      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST, "REEF app command: {0}", command);
      }

      this.hdInsightInstance.submitApplication(applicationSubmission);

    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Error submitting HDInsight request", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Extracts the resource demands from the jobSubmissionProto.
   */
  private final Resource getResource(
      final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {

    return new Resource()
        .setMemory(String.valueOf(jobSubmissionProto.getDriverMemory()))
        .setvCores("1");
  }

  /**
   * Assembles the command to execute the Driver.
   */
  private String getCommandString(
      final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {
    return StringUtils.join(getCommandList(jobSubmissionProto), ' ');
  }

  /**
   * Assembles the command to execute the Driver in list form.
   */
  private List<String> getCommandList(
      final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {

    return new JavaLaunchCommandBuilder()
        .setJavaPath("%JAVA_HOME%/bin/java")
        .setErrorHandlerRID(jobSubmissionProto.getRemoteId())
        .setLaunchID(jobSubmissionProto.getIdentifier())
        .setConfigurationFileName(filenames.getDriverConfigurationPath())
        .setClassPath(getClassPath())
        .setMemory(jobSubmissionProto.getDriverMemory())
        .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + filenames.getDriverStderrFileName())
        .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + filenames.getDriverStdoutFileName())
        .build();
  }

  private String getClassPath() {
    return StringUtils.join(Arrays.asList(
        "%HADOOP_HOME%/etc/hadoop",
        "%HADOOP_HOME%/share/hadoop/common/*",
        "%HADOOP_HOME%/share/hadoop/common/lib/*",
        "%HADOOP_HOME%/share/hadoop/yarn/*",
        "%HADOOP_HOME%/share/hadoop/yarn/lib/*",
        this.filenames.getClasspath()), File.pathSeparatorChar);
  }

  private Configuration makeDriverConfiguration(
      final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto,
      final String applicationId,
      final String jobFolderURL) throws IOException {

    final Configuration hdinsightDriverConfiguration = HDInsightDriverConfiguration.CONF
        .set(HDInsightDriverConfiguration.JOB_IDENTIFIER, applicationId)
        .set(HDInsightDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobFolderURL)
        .build();

    return Configurations.merge(
        this.configurationSerializer.fromString(jobSubmissionProto.getConfiguration()),
        hdinsightDriverConfiguration);
  }
}
