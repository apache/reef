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
package com.microsoft.reef.runtime.yarn.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.files.JobJarMaker;
import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.yarn.driver.YarnMasterConfiguration;
import com.microsoft.reef.runtime.yarn.util.YarnTypes;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());
  private final YarnConfiguration yarnConfiguration;
  private final YarnClient yarnClient;
  private final JobJarMaker jobJarMaker;
  private final REEFFileNames fileNames;
  private final FileSystem fileSystem;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  YarnJobSubmissionHandler(final YarnConfiguration yarnConfiguration,
                           final JobJarMaker jobJarMaker,
                           final REEFFileNames fileNames,
                           final ConfigurationSerializer configurationSerializer) throws IOException {
    this.yarnConfiguration = yarnConfiguration;
    this.jobJarMaker = jobJarMaker;
    this.fileNames = fileNames;
    this.configurationSerializer = configurationSerializer;

    this.fileSystem = FileSystem.get(yarnConfiguration);

    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);
    this.yarnClient.start();
  }

  @Override
  public void close() {
    this.yarnClient.stop();
  }

  @Override
  public void onNext(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {
    LOG.log(Level.FINEST, "Submitting job with ID [{0}]", jobSubmissionProto.getIdentifier());
    try {
      LOG.log(Level.FINE, "Requesting Application ID from YARN.");
      final YarnClientApplication yarnClientApplication = yarnClient.createApplication();
      final GetNewApplicationResponse applicationResponse = yarnClientApplication.getNewApplicationResponse();

      final ApplicationSubmissionContext applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
      final ApplicationId applicationId = applicationSubmissionContext.getApplicationId();

      LOG.log(Level.FINEST, "YARN Application ID: {0}", applicationId);

      // set the application name
      applicationSubmissionContext.setApplicationName("reef-job-" + jobSubmissionProto.getIdentifier());


      LOG.log(Level.INFO, "Assembling submission JAR for the Driver.");
      final Path submissionFolder = new Path("/tmp/" + fileNames.getJobFolderPrefix() + applicationId.getId() + "/");
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionProto, submissionFolder);
      final File jobSubmissionFile = this.jobJarMaker.createJobSubmissionJAR(jobSubmissionProto, driverConfiguration);
      final Path uploadedJobJarPath = this.uploadToJobFolder(jobSubmissionFile, submissionFolder);

      final Map<String, LocalResource> resources = new HashMap<>(1);
      resources.put(fileNames.getREEFFolderName(), this.makeLocalResourceForJarFile(uploadedJobJarPath));


      // SET MEMORY RESOURCE
      final int amMemory = getMemory(jobSubmissionProto, applicationResponse.getMaximumResourceCapability().getMemory());
      applicationSubmissionContext.setResource(Resource.newInstance(amMemory, 1));

      // SET EXEC COMMAND
      final List<String> launchCommand = new JavaLaunchCommandBuilder()
          .setErrorHandlerRID(jobSubmissionProto.getRemoteId())
          .setLaunchID(jobSubmissionProto.getIdentifier())
          .setConfigurationFileName(fileNames.getDriverConfigurationPath())
          .setClassPath(fileNames.getClasspath())
          .setMemory(amMemory)
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + fileNames.getDriverStdoutFileName())
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + fileNames.getDriverStderrFileName())
          .build();

      applicationSubmissionContext.setAMContainerSpec(YarnTypes.getContainerLaunchContext(launchCommand, resources));
      applicationSubmissionContext.setPriority(getPriority(jobSubmissionProto));

      // Set the queue to which this application is to be submitted in the RM
      applicationSubmissionContext.setQueue(getQueue(jobSubmissionProto, "default"));
      LOG.log(Level.INFO, "Submitting REEF Application to YARN. ID: {0}", applicationId);
      this.yarnClient.submitApplication(applicationSubmissionContext);
    } catch (final YarnException | IOException e) {
      throw new RuntimeException("Unable to submit Driver to YARN.", e);
    }
  }

  /**
   * Assembles the Driver configuration.
   *
   * @param jobSubmissionProto
   * @param jobFolderPath
   * @return
   * @throws IOException
   */
  private Configuration makeDriverConfiguration(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto,
                                                final Path jobFolderPath) throws IOException {
    return new YarnMasterConfiguration()
        .setJobSubmissionDirectory(jobFolderPath.toString())
        .setJobIdentifier(jobSubmissionProto.getIdentifier())
        .setClientRemoteIdentifier(jobSubmissionProto.getRemoteId())
        .addClientConfiguration(this.configurationSerializer.fromString(jobSubmissionProto.getConfiguration()))
        .build();
  }


  private final Path uploadToJobFolder(final File file, final Path jobFolder) throws IOException {
    final Path source = new Path(file.getAbsolutePath());
    final Path destination = new Path(jobFolder, file.getName());
    LOG.log(Level.INFO, "Uploading {0} to {1}", new Object[]{source, destination});
    this.fileSystem.copyFromLocalFile(false, true, source, destination);
    return destination;
  }


  private Priority getPriority(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {
    return Priority.newInstance(jobSubmissionProto.hasPriority() ? jobSubmissionProto.getPriority() : 0);
  }

  /**
   * Extract the queue name from the jobSubmissionProto or return default if none is set.
   *
   * @param jobSubmissionProto
   * @param defaultQueue
   * @return
   */
  // TODO: Revisit this. We also have a named parameter for the queue in YarnClientConfiguration
  private final String getQueue(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto,
                                final String defaultQueue) {
    return jobSubmissionProto.hasQueue() && !jobSubmissionProto.getQueue().isEmpty() ?
        jobSubmissionProto.getQueue() : defaultQueue;
  }

  /**
   * Extract the desired driver memory from jobSubmissionProto.
   * <p/>
   * returns maxMemory if that desired amount is more than maxMemory
   *
   * @param jobSubmissionProto
   * @param maxMemory
   * @return
   */

  private int getMemory(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto,
                        final int maxMemory) {
    final int amMemory;
    final int requestedMemory = jobSubmissionProto.getDriverMemory();
    if (requestedMemory <= maxMemory) {
      amMemory = requestedMemory;
    } else {
      LOG.log(Level.WARNING, "Requested {0}MB of memory for the driver. The max on this YARN installation is {1}. Using {1} as the memory for the driver.",
          new Object[]{requestedMemory, maxMemory});
      amMemory = maxMemory;
    }
    return amMemory;
  }


  /**
   * Creates a LocalResource instance for the JAR file referenced by the given Path
   *
   * @param path
   * @return
   * @throws IOException
   */
  private LocalResource makeLocalResourceForJarFile(final Path path) throws IOException {
    final LocalResource localResource = Records.newRecord(LocalResource.class);
    final FileStatus status = FileContext.getFileContext(fileSystem.getUri()).getFileStatus(path);
    localResource.setType(LocalResourceType.ARCHIVE);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(status.getPath()));
    localResource.setTimestamp(status.getModificationTime());
    localResource.setSize(status.getLen());
    return localResource;
  }

}
