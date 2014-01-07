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
package com.microsoft.reef.runtime.local.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.local.driver.LocalDriverConfiguration;
import com.microsoft.reef.runtime.local.driver.LocalDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.local.driver.RunnableProcess;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Job Submissions for the Local Runtime.
 */
@Private
@ClientSide
final class LocalJobSubmissionHandler implements JobSubmissionHandler {
  /**
   * The name of the folder for the driver within the Job folder.
   */
  public static final String DRIVER_FOLDER_NAME = "driver";

  /**
   * The file name used to store the driver configuration with the driver folder.
   */
  public static final String DRIVER_CONFIGURATION_FILE_NAME = "driver.conf";

  /**
   * The environment variable to use for the root folder.
   */
  public static final String ROOT_FOLDER_ENV_KEY = "com.microsoft.reef.runtime.local.folder";

  /**
   * The (hard-coded) amount of memory to be used for the driver.
   */
  public static final int DRIVER_MEMORY = 512;
  private static final Logger LOG = Logger.getLogger(LocalJobSubmissionHandler.class.getName());
  private final ExecutorService executor;
  private final int nThreads;
  private final String rootFolderName;


  @Inject
  public LocalJobSubmissionHandler(final ExecutorService executor,
                                   final @Parameter(LocalRuntimeConfiguration.RootFolder.class) String rootFolderName,
                                   final @Parameter(LocalRuntimeConfiguration.NumberOfThreads.class) int nThreads) {
    this.executor = executor;
    this.nThreads = nThreads;
    this.rootFolderName = new File(rootFolderName).getAbsolutePath();
  }

  @Inject
  public LocalJobSubmissionHandler(final ExecutorService executor,
                                   final @Parameter(LocalRuntimeConfiguration.NumberOfThreads.class) int nThreads) {
    this(executor, System.getProperty(ROOT_FOLDER_ENV_KEY, "REEF_LOCAL_RUNTIME"), nThreads);
  }


  @Override
  public final void close() {
    this.executor.shutdown();
  }

  @Override
  public final void onNext(final ClientRuntimeProtocol.JobSubmissionProto t) {
    try {
      LOG.log(Level.FINEST, "Starting Job {0}", t.getIdentifier());
      final File jobFolder = new File(new File(rootFolderName), "/" + t.getIdentifier() + "-" + System.currentTimeMillis() + "/");
      final File driverFolder = new File(jobFolder, DRIVER_FOLDER_NAME);
      driverFolder.mkdirs();

      final DriverFiles driverFiles = DriverFiles.fromJobSubmission(t);
      driverFiles.copyTo(driverFolder);

      ConfigurationModule driverConfigurationPart1 = LocalDriverConfiguration.CONF
          .set(LocalDriverConfiguration.NUMBER_OF_PROCESSES, this.nThreads)
          .set(LocalDriverConfiguration.ROOT_FOLDER, jobFolder.getAbsolutePath());
      driverConfigurationPart1 = driverFiles.addNamesTo(driverConfigurationPart1,
          LocalDriverConfiguration.GLOBAL_FILES,
          LocalDriverConfiguration.GLOBAL_LIBRARIES,
          LocalDriverConfiguration.LOCAL_FILES,
          LocalDriverConfiguration.LOCAL_LIBRARIES);


      final Configuration driverConfigurationPart2 = new LocalDriverRuntimeConfiguration()
          .addClientConfiguration(TANGUtils.fromString(t.getConfiguration()))
          .setClientRemoteIdentifier(t.getRemoteId())
          .setJobIdentifier(t.getIdentifier()).build();

      final Configuration driverConfiguration = TANGUtils.merge(driverConfigurationPart2, driverConfigurationPart1.build());
      final File runtimeConfigurationFile = new File(driverFolder, DRIVER_CONFIGURATION_FILE_NAME);
      ConfigurationFile.writeConfigurationFile(driverConfiguration, runtimeConfigurationFile);

      final List<String> command = new JavaLaunchCommandBuilder()
          .setErrorHandlerRID(t.getRemoteId())
          .setLaunchID(t.getIdentifier())
          .setConfigurationFileName(DRIVER_CONFIGURATION_FILE_NAME)
          .setClassPath(driverFiles.getClassPath())
          .setMemory(DRIVER_MEMORY)
          .build();

      final RunnableProcess process = new RunnableProcess(command, "driver", driverFolder);
      this.executor.submit(process);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Unable to setup driver.", e);
      throw new RuntimeException("Unable to setup driver.", e);
    }
  }


}