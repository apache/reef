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

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Job Submissions for the Local Runtime.
 */
@Private
@ClientSide
final class LocalJobSubmissionHandler implements JobSubmissionHandler {


  private static final Logger LOG = Logger.getLogger(LocalJobSubmissionHandler.class.getName());
  private final ExecutorService executor;
  private final String rootFolderName;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;
  private final PreparedDriverFolderLauncher driverLauncher;
  private final LoggingScopeFactory loggingScopeFactory;
  private final DriverConfigurationProvider driverConfigurationProvider;

  @Inject
  LocalJobSubmissionHandler(
      final ExecutorService executor,
      @Parameter(RootFolder.class) final String rootFolderName,
      final ConfigurationSerializer configurationSerializer,
      final REEFFileNames fileNames,

      final PreparedDriverFolderLauncher driverLauncher,
      final LoggingScopeFactory loggingScopeFactory,
      final DriverConfigurationProvider driverConfigurationProvider) {

    this.executor = executor;
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;

    this.driverLauncher = driverLauncher;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.rootFolderName = new File(rootFolderName).getAbsolutePath();
    this.loggingScopeFactory = loggingScopeFactory;

    LOG.log(Level.FINE, "Instantiated 'LocalJobSubmissionHandler'");
  }

  @Override
  public void close() {
    this.executor.shutdown();
  }

  @Override
  public void onNext(final JobSubmissionEvent t) {
    try (final LoggingScope lf = loggingScopeFactory.localJobSubmission()) {
      try {
        LOG.log(Level.FINEST, "Starting local job {0}", t.getIdentifier());

        final File jobFolder = new File(new File(rootFolderName),
            "/" + t.getIdentifier() + "-" + System.currentTimeMillis() + "/");

        final File driverFolder = new File(jobFolder, PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);
        if (!driverFolder.exists() && !driverFolder.mkdirs()) {
          LOG.log(Level.WARNING, "Failed to create [{0}]", driverFolder.getAbsolutePath());
        }

        final DriverFiles driverFiles = DriverFiles.fromJobSubmission(t, this.fileNames);
        driverFiles.copyTo(driverFolder);

        final Configuration driverConfiguration = this.driverConfigurationProvider
            .getDriverConfiguration(jobFolder.getAbsolutePath(),
                                    t.getRemoteId(),
                                    t.getIdentifier(),
                                    t.getConfiguration());

        this.configurationSerializer.toFile(driverConfiguration,
            new File(driverFolder, this.fileNames.getDriverConfigurationPath()));
        this.driverLauncher.launch(driverFolder, t.getIdentifier(), t.getRemoteId());
      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Unable to setup driver.", e);
        throw new RuntimeException("Unable to setup driver.", e);
      }
    }
  }
}
