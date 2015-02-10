/**
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
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.ClientRuntimeProtocol;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.local.client.parameters.NumberOfProcesses;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.local.driver.LocalDriverRuntimeConfiguration;
import org.apache.reef.runtime.local.process.LoggingRunnableProcessObserver;
import org.apache.reef.runtime.local.process.RunnableProcess;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

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
   * The (hard-coded) amount of memory to be used for the driver.
   */
  public static final int DRIVER_MEMORY = 512;
  private static final Logger LOG = Logger.getLogger(LocalJobSubmissionHandler.class.getName());
  private final ExecutorService executor;
  private final int nThreads;
  private final String rootFolderName;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames filenames;
  private final ClasspathProvider classpath;
  private final double jvmHeapSlack;
  private final LoggingScopeFactory loggingScopeFactory;

  @Inject
  public LocalJobSubmissionHandler(
      final ExecutorService executor,
      final @Parameter(RootFolder.class) String rootFolderName,
      final @Parameter(NumberOfProcesses.class) int nThreads,
      final ConfigurationSerializer configurationSerializer,
      final REEFFileNames filenames,
      final ClasspathProvider classpath,
      final @Parameter(JVMHeapSlack.class) double jvmHeapSlack,
      final LoggingScopeFactory loggingScopeFactory) {

    this.executor = executor;
    this.nThreads = nThreads;
    this.configurationSerializer = configurationSerializer;
    this.filenames = filenames;
    this.classpath = classpath;
    this.jvmHeapSlack = jvmHeapSlack;
    this.rootFolderName = new File(rootFolderName).getAbsolutePath();
    this.loggingScopeFactory = loggingScopeFactory;

    LOG.log(Level.FINE, "Instantiated 'LocalJobSubmissionHandler'");
  }

  @Override
  public final void close() {
    this.executor.shutdown();
  }

  @Override
  public final void onNext(final ClientRuntimeProtocol.JobSubmissionProto t) {
    try (final LoggingScope lf = loggingScopeFactory.localJobSubmission()) {
      try {
        LOG.log(Level.FINEST, "Starting local job {0}", t.getIdentifier());

        final File jobFolder = new File(new File(rootFolderName),
            "/" + t.getIdentifier() + "-" + System.currentTimeMillis() + "/");

        final File driverFolder = new File(jobFolder, DRIVER_FOLDER_NAME);
        driverFolder.mkdirs();

        final DriverFiles driverFiles = DriverFiles.fromJobSubmission(t, this.filenames);
        driverFiles.copyTo(driverFolder);

        final Configuration driverConfigurationPart1 = driverFiles
            .addNamesTo(LocalDriverConfiguration.CONF,
                LocalDriverConfiguration.GLOBAL_FILES,
                LocalDriverConfiguration.GLOBAL_LIBRARIES,
                LocalDriverConfiguration.LOCAL_FILES,
                LocalDriverConfiguration.LOCAL_LIBRARIES)
            .set(LocalDriverConfiguration.NUMBER_OF_PROCESSES, this.nThreads)
            .set(LocalDriverConfiguration.ROOT_FOLDER, jobFolder.getAbsolutePath())
            .set(LocalDriverConfiguration.JVM_HEAP_SLACK, this.jvmHeapSlack)
            .build();

        final Configuration driverConfigurationPart2 = new LocalDriverRuntimeConfiguration()
            .addClientConfiguration(this.configurationSerializer.fromString(t.getConfiguration()))
            .setClientRemoteIdentifier(t.getRemoteId())
            .setJobIdentifier(t.getIdentifier()).build();

        final Configuration driverConfiguration = Tang.Factory.getTang()
            .newConfigurationBuilder(driverConfigurationPart1, driverConfigurationPart2).build();
        final File runtimeConfigurationFile = new File(driverFolder, this.filenames.getDriverConfigurationPath());
        this.configurationSerializer.toFile(driverConfiguration, runtimeConfigurationFile);

        final List<String> command = new JavaLaunchCommandBuilder()
            .setErrorHandlerRID(t.getRemoteId())
            .setLaunchID(t.getIdentifier())
            .setConfigurationFileName(this.filenames.getDriverConfigurationPath())
            .setClassPath(this.classpath.getDriverClasspath())
            .setMemory(DRIVER_MEMORY)
            .build();

        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "REEF app command: {0}", StringUtils.join(command, ' '));
        }

        final RunnableProcess process = new RunnableProcess(command,
            "driver",
            driverFolder,
            new LoggingRunnableProcessObserver(),
            this.filenames.getDriverStdoutFileName(),
            this.filenames.getDriverStderrFileName());
        this.executor.submit(process);
        this.executor.shutdown();

      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Unable to setup driver.", e);
        throw new RuntimeException("Unable to setup driver.", e);
      }
    }
  }
}
