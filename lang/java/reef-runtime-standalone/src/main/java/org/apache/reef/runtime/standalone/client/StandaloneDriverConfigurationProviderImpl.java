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
package org.apache.reef.runtime.standalone.client;

import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.standalone.client.parameters.NodeFolder;
import org.apache.reef.runtime.standalone.client.parameters.NodeListFilePath;
import org.apache.reef.runtime.standalone.client.parameters.SshPortNum;
import org.apache.reef.runtime.standalone.driver.StandaloneDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;

import javax.inject.Inject;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that assembles the driver configuration when run on the local runtime.
 */
final class StandaloneDriverConfigurationProviderImpl implements DriverConfigurationProvider {

  private static final Logger LOG = Logger.getLogger(StandaloneDriverConfigurationProviderImpl.class.getName());
  private final double jvmHeapSlack;
  private final String nodeListFilePath;
  private final String nodeFolder;
  private final int sshPortNum;
  private final Set<String> nodeInfoSet;

  @Inject
  StandaloneDriverConfigurationProviderImpl(@Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
                                            @Parameter(NodeListFilePath.class) final String nodeListFilePath,
                                            @Parameter(NodeFolder.class) final String nodeFolder,
                                            @Parameter(SshPortNum.class) final int sshPortNum) {
    this.jvmHeapSlack = jvmHeapSlack;
    this.nodeListFilePath = nodeListFilePath;
    this.nodeFolder = nodeFolder;
    this.sshPortNum = sshPortNum;
    this.nodeInfoSet = new HashSet<>();

    LOG.log(Level.FINEST, "Reading NodeListFilePath");
    try {
      final InputStream in = new FileInputStream(this.nodeListFilePath);
      final Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
      final BufferedReader br = new BufferedReader(reader);
      while (true) {
        final String line = br.readLine();
        if (line == null) {
          break;
        }
        this.nodeInfoSet.add(line);
      }
      br.close();
    } catch (final FileNotFoundException ex) {
      LOG.log(Level.SEVERE, "Failed to open file in NodeListFilePath: {0}", nodeListFilePath);
      throw new RuntimeException("Failed to open file in NodeListFilePath: " + nodeListFilePath, ex);
    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Failed to read file");
      throw new RuntimeException("Failed to read file", ex);
    }
  }

  private Configuration getDriverConfiguration(final URI jobFolder,
                                               final String clientRemoteId,
                                               final String jobId) {
    ConfigurationModule configModule = StandaloneDriverConfiguration.CONF
        .set(StandaloneDriverConfiguration.ROOT_FOLDER, jobFolder.getPath())
        .set(StandaloneDriverConfiguration.NODE_FOLDER, this.nodeFolder)
        .set(StandaloneDriverConfiguration.NODE_LIST_FILE_PATH, this.nodeListFilePath)
        .set(StandaloneDriverConfiguration.SSH_PORT_NUM, this.sshPortNum)
        .set(StandaloneDriverConfiguration.JVM_HEAP_SLACK, this.jvmHeapSlack)
        .set(StandaloneDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
        .set(StandaloneDriverConfiguration.JOB_IDENTIFIER, jobId);
    for (final String nodeInfo : nodeInfoSet) {
      configModule = configModule.set(StandaloneDriverConfiguration.NODE_INFO_SET, nodeInfo);
    }
    return configModule.build();
  }

  /**
   * Assembles the driver configuration.
   *
   * @param jobFolder                The folder in which the local runtime will execute this job.
   * @param clientRemoteId           the remote identifier of the client. It is used by the Driver to establish a
   *                                 connection back to the client.
   * @param jobId                    The identifier of the job.
   * @param applicationConfiguration The configuration of the application, e.g. a filled out DriverConfiguration
   * @return The Driver configuration to be used to instantiate the Driver.
   */
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(getDriverConfiguration(jobFolder, clientRemoteId, jobId), applicationConfiguration);
  }
}
