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
package org.apache.reef.runtime.mesos.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.mesos.client.parameters.RootFolder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The current implementation runs the driver as a local process, similar to reef-runtime-local.
 * TODO[JIRA REEF-98]: run the driver on a slave node in the cluster
 */
@Private
@ClientSide
final class MesosJobSubmissionHandler implements JobSubmissionHandler {
  private static final Logger LOG = Logger.getLogger(MesosJobSubmissionHandler.class.getName());

  public static final String DRIVER_FOLDER_NAME = "driver";

  private final ConfigurationSerializer configurationSerializer;
  private final ClasspathProvider classpath;
  private final REEFFileNames fileNames;
  private final String rootFolderName;
  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  MesosJobSubmissionHandler(@Parameter(RootFolder.class) final String rootFolderName,
                            final ConfigurationSerializer configurationSerializer,
                            final REEFFileNames fileNames,
                            final ClasspathProvider classpath,
                            final DriverConfigurationProvider driverConfigurationProvider) {
    this.rootFolderName = new File(rootFolderName).getAbsolutePath();
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {
    try {
      final File jobFolder = new File(new File(this.rootFolderName),
              "/" + jobSubmissionEvent.getIdentifier() + "-" + System.currentTimeMillis() + "/");

      final File driverFolder = new File(jobFolder, DRIVER_FOLDER_NAME);
      if (!driverFolder.exists() && !driverFolder.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create folder {0}", driverFolder.getAbsolutePath());
      }

      final File reefFolder = new File(driverFolder, this.fileNames.getREEFFolderName());
      if (!reefFolder.exists() && !reefFolder.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create folder {0}", reefFolder.getAbsolutePath());
      }

      final File localFolder = new File(reefFolder, this.fileNames.getLocalFolderName());
      if (!localFolder.exists() && !localFolder.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create folder {0}", localFolder.getAbsolutePath());
      }

      for (final FileResource file : jobSubmissionEvent.getLocalFileSet()) {
        final Path src = new File(file.getPath()).toPath();
        final Path dst = new File(driverFolder, this.fileNames.getLocalFolderPath() + "/" + file.getName()).toPath();
        Files.copy(src, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

      final File globalFolder = new File(reefFolder, this.fileNames.getGlobalFolderName());
      if (!globalFolder.exists() && !globalFolder.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create folder {0}", globalFolder.getAbsolutePath());
      }

      for (final FileResource file : jobSubmissionEvent.getGlobalFileSet()) {
        final Path src = new File(file.getPath()).toPath();
        final Path dst = new File(driverFolder, this.fileNames.getGlobalFolderPath() + "/" + file.getName()).toPath();
        Files.copy(src, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }

      final Configuration driverConfiguration = this.driverConfigurationProvider.getDriverConfiguration(
              "",
              jobSubmissionEvent.getRemoteId(),
              jobSubmissionEvent.getIdentifier(),
              jobSubmissionEvent.getConfiguration());

      final File runtimeConfigurationFile = new File(driverFolder, this.fileNames.getDriverConfigurationPath());
      this.configurationSerializer.toFile(driverConfiguration, runtimeConfigurationFile);

      final List<String> launchCommand = new JavaLaunchCommandBuilder()
          .setConfigurationFilePaths(Collections.singletonList(this.fileNames.getDriverConfigurationPath()))
          .setClassPath(this.classpath.getDriverClasspath())
          .setMemory(jobSubmissionEvent.getDriverMemory().get())
          .build();

      final File errFile = new File(driverFolder, fileNames.getDriverStderrFileName());
      final File outFile = new File(driverFolder, fileNames.getDriverStdoutFileName());

      new ProcessBuilder()
              .command(launchCommand)
              .directory(driverFolder)
              .redirectError(errFile)
              .redirectOutput(outFile)
              .start();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
