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
package org.apache.reef.runtime.local.client;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.local.process.LoggingRunnableProcessObserver;
import org.apache.reef.runtime.local.process.RunnableProcess;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;

/**
 * Launcher for a already prepared driver folder.
 */
public class PreparedDriverFolderLauncher {

  /**
   * The name of the folder for the driver within the Job folder.
   */
  public static final String DRIVER_FOLDER_NAME = "driver";

  private final ExecutorService executor;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final List<String> commandPrefixList;

  /**
   * The (hard-coded) amount of memory to be used for the driver.
   */
  public static final int DRIVER_MEMORY = 512;

  private static final Logger LOG = Logger.getLogger(PreparedDriverFolderLauncher.class.getName());

  @Inject
  PreparedDriverFolderLauncher(final ExecutorService executor, final REEFFileNames fileNames,
                               @Parameter(DriverLaunchCommandPrefix.class) final List<String> commandPrefixList,
                               final ClasspathProvider classpath) {
    this.executor = executor;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.commandPrefixList = commandPrefixList;
  }

  /**
   * Launches the driver prepared in driverFolder.
   *
   * @param driverFolder
   * @param jobId
   * @param clientRemoteId
   */
  public void launch(final File driverFolder, final String jobId, final String clientRemoteId) {
    assert driverFolder.isDirectory();

    final List<String> command = makeLaunchCommand(jobId, clientRemoteId);

    final RunnableProcess process = new RunnableProcess(command,
        "driver",
        driverFolder,
        new LoggingRunnableProcessObserver(),
        this.fileNames.getDriverStdoutFileName(),
        this.fileNames.getDriverStderrFileName());
    this.executor.submit(process);
    this.executor.shutdown();
  }

  private List<String> makeLaunchCommand(final String jobId, final String clientRemoteId) {

    final List<String> command = new JavaLaunchCommandBuilder(commandPrefixList)
        .setConfigurationFilePaths(Collections.singletonList(this.fileNames.getDriverConfigurationPath()))
        .setClassPath(this.classpath.getDriverClasspath())
        .setMemory(DRIVER_MEMORY)
        .build();

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "REEF app command: {0}", StringUtils.join(command, ' '));
    }
    return command;
  }

}
