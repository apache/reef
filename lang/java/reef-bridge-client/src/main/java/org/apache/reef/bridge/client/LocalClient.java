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
package org.apache.reef.bridge.client;

import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.local.client.PreparedDriverFolderLauncher;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Submits a folder containing a Driver to the local runtime.
 */
public final class LocalClient {

  private static final Logger LOG = Logger.getLogger(LocalClient.class.getName());
  private static final String CLIENT_REMOTE_ID = ClientRemoteIdentifier.NONE;
  private final PreparedDriverFolderLauncher launcher;
  private final LocalRuntimeDriverConfigurationGenerator configurationGenerator;

  @Inject
  private LocalClient(final PreparedDriverFolderLauncher launcher,
                      final LocalRuntimeDriverConfigurationGenerator configurationGenerator) {
    this.launcher = launcher;
    this.configurationGenerator = configurationGenerator;
  }

  private void submit(final LocalSubmissionFromCS localSubmissionFromCS) throws IOException {
    final File driverFolder = new File(localSubmissionFromCS.getJobFolder(),
        PreparedDriverFolderLauncher.DRIVER_FOLDER_NAME);
    if (!driverFolder.exists()) {
      throw new IOException("The Driver folder " + driverFolder.getAbsolutePath() + " doesn't exist.");
    }

    configurationGenerator.writeConfiguration(localSubmissionFromCS.getJobFolder(),
        localSubmissionFromCS.getJobId(), CLIENT_REMOTE_ID);
    launcher.launch(driverFolder, localSubmissionFromCS.getJobId(), CLIENT_REMOTE_ID);
  }

  public static void main(final String[] args) throws IOException, InjectionException {
    final File jobSubmissionParametersFile = new File(args[0]);
    final File localAppSubmissionParametersFile = new File(args[1]);

    if (!(jobSubmissionParametersFile.exists() && jobSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + jobSubmissionParametersFile.getAbsolutePath());
    }

    if (!(localAppSubmissionParametersFile.exists() && localAppSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + localAppSubmissionParametersFile.getAbsolutePath());
    }

    final LocalSubmissionFromCS localSubmissionFromCS =
        LocalSubmissionFromCS.fromSubmissionParameterFiles(
            jobSubmissionParametersFile, localAppSubmissionParametersFile);
    LOG.log(Level.INFO, "Local job submission received from C#: {0}", localSubmissionFromCS);
    final Configuration runtimeConfiguration = localSubmissionFromCS.getRuntimeConfiguration();

    final LocalClient client = Tang.Factory.getTang()
        .newInjector(runtimeConfiguration)
        .getInstance(LocalClient.class);

    client.submit(localSubmissionFromCS);
  }
}