/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.yarn.master.YarnMasterConfiguration;
import com.microsoft.reef.runtime.yarn.util.YarnUtils;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationFile;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


@Private
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());
  private final static int MIN_REEF_MASTER_MEMORY = 512;
  private final YarnConfiguration yarnConfiguration;
  private final YarnClient yarnClient;
  private final Path reefJarFile;

  @Inject
  YarnJobSubmissionHandler(final YarnConfiguration yarnConfiguration,
                           @Parameter(YarnClientConfiguration.ReefJarFile.class) final String reefJarFile) {
    this.yarnConfiguration = yarnConfiguration;
    this.reefJarFile = new Path(reefJarFile);


    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);
    this.yarnClient.start();
  }

  @Override
  public void close() {
    this.yarnClient.stop();
  }

  @Override
  public void onNext(ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) {
    try {
      // Get a new application id
      YarnClientApplication yarnClientApplication = yarnClient.createApplication();
      GetNewApplicationResponse applicationResponse = yarnClientApplication.getNewApplicationResponse();

      final ApplicationSubmissionContext applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
      final ApplicationId applicationId = applicationSubmissionContext.getApplicationId();

      LOG.info("YARN Application ID: " + applicationId);

      // set the application name
      final String jobName = "reef-job-" + jobSubmissionProto.getIdentifier();
      applicationSubmissionContext.setApplicationName(jobName);

      final FileSystem fs = FileSystem.get(this.yarnConfiguration);
      final String root_dir = "/tmp/reef-" + jobSubmissionProto.getUserName();
      final Path job_dir = new Path(root_dir, jobName + "/" + applicationId.getId() + "/");
      final Path global_dir = new Path(job_dir, YarnMasterConfiguration.GLOBAL_FILE_DIRECTORY);

      ///////////////////////////////////////////////////////////////////////
      // FILE RESOURCES
      final Map<String, LocalResource> localResources = new HashMap<>();

      final File yarnConfigurationFile = File.createTempFile("yarn", ".conf");
      final FileOutputStream yarnConfigurationFOS = new FileOutputStream(yarnConfigurationFile);
      this.yarnConfiguration.writeXml(yarnConfigurationFOS);
      yarnConfigurationFOS.close();

      LOG.info("Upload tmp yarn configuration file " + yarnConfigurationFile.toURI());
      localResources.put(yarnConfigurationFile.getName(),
          YarnUtils.getLocalResource(fs, new Path(yarnConfigurationFile.toURI()), new Path(global_dir, yarnConfigurationFile.getName())));

      final StringBuilder globalClassPath = YarnUtils.getClassPathBuilder(this.yarnConfiguration);

      localResources.put(this.reefJarFile.getName(),
          YarnUtils.getLocalResource(fs, this.reefJarFile, new Path(global_dir, this.reefJarFile.getName())));
      globalClassPath.append(File.pathSeparatorChar + this.reefJarFile.getName());

      for (ReefServiceProtos.FileResourceProto file : jobSubmissionProto.getGlobalFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(global_dir, file.getName());
        switch (file.getType()) {
          case PLAIN:
            LOG.info("GLOBAL FILE RESOURCE: upload " + src + " to " + dst);
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case LIB:
            globalClassPath.append(File.pathSeparatorChar + file.getName());
            LOG.info("GLOBAL LIB FILE RESOURCE: upload " + src + " to " + dst);
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
        }
      }

      final StringBuilder localClassPath = new StringBuilder();

      for (ReefServiceProtos.FileResourceProto file : jobSubmissionProto.getLocalFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(job_dir, file.getName());
        switch (file.getType()) {
          case PLAIN:
            LOG.info("LOCAL FILE RESOURCE: upload " + src + " to " + dst);
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case LIB:
            localClassPath.append(File.pathSeparatorChar + file.getName());
            LOG.info("LOCAL LIB FILE RESOURCE: upload " + src + " to " + dst);
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(fs, src, dst));
            break;
        }
      }

      // RUNTIME CONFIGURATION FILE
      final Configuration masterConfiguration = new YarnMasterConfiguration()
          .setGlobalFileClassPath(globalClassPath.toString())
          .setJobSubmissionDirectory(job_dir.toString())
          .setYarnConfigurationFile(yarnConfigurationFile.getName())
          .setJobIdentifier(jobSubmissionProto.getIdentifier())
          .setClientRemoteIdentifier(jobSubmissionProto.getRemoteId())
          .addClientConfiguration(TANGUtils.fromString(jobSubmissionProto.getConfiguration()))
          .build();
      final File masterConfigurationFile = File.createTempFile("driver", ".conf");
      ConfigurationFile.writeConfigurationFile(masterConfiguration, masterConfigurationFile);

      localResources.put(masterConfigurationFile.getName(),
          YarnUtils.getLocalResource(fs, new Path(masterConfigurationFile.toURI()), new Path(job_dir, masterConfigurationFile.getName())));

      ////////////////////////////////////////////////////////////////////////////

      // SET MEMORY RESOURCE
      final int amMemory = YarnUtils.getMemorySize(jobSubmissionProto.hasDriverSize() ? jobSubmissionProto.getDriverSize() : ReefServiceProtos.SIZE.SMALL,
          MIN_REEF_MASTER_MEMORY, applicationResponse.getMaximumResourceCapability().getMemory());
      final Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(amMemory);
      applicationSubmissionContext.setResource(capability);

      final String classPath = "".equals(localClassPath.toString()) ?
          globalClassPath.toString() : localClassPath.toString() + File.pathSeparatorChar + globalClassPath.toString();

      // SET EXEC COMMAND
      final List<String> launchCommandList = new JavaLaunchCommandBuilder()
          .setErrorHandlerRID(jobSubmissionProto.getRemoteId())
          .setLaunchID(jobSubmissionProto.getIdentifier())
          .setConfigurationFileName(masterConfigurationFile.getName())
          .setClassPath(classPath)
          .setMemory(amMemory)
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/driver.stdout")
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/driver.stderr")
          .build();
      final String launchCommand = StringUtils.join(launchCommandList, ' ');
      LOG.info("LAUNCH COMMAND: " + launchCommand);

      final ContainerLaunchContext containerContext = YarnUtils.getContainerLaunchContext(launchCommand, localResources);
      applicationSubmissionContext.setAMContainerSpec(containerContext);

      final Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(jobSubmissionProto.hasPriority() ? jobSubmissionProto.getPriority() : 0);
      applicationSubmissionContext.setPriority(pri);

      // Set the queue to which this application is to be submitted in the RM
      applicationSubmissionContext.setQueue(jobSubmissionProto.hasQueue() ? jobSubmissionProto.getQueue() : "default");

      LOG.info("Submit REEF Application to YARN");
      this.yarnClient.submitApplication(applicationSubmissionContext);
      // monitorApplication(applicationId);
    } catch (YarnException | IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean monitorApplication(ApplicationId appId)
      throws YarnException, IOException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warning("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }

    }

  }
}
