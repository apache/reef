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
package org.apache.reef.runtime.yarn.driver.unmanaged;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test REEF Driver in Unmanaged AM mode on YARN.
 */
public final class UnmanagedAmTest implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

  private static final Logger LOG = Logger.getLogger(UnmanagedAmTest.class.getName());

  @Test
  public void testAmShutdown() throws IOException, YarnException {

    Assume.assumeTrue(
        "This test requires a YARN Resource Manager to connect to",
        Boolean.parseBoolean(System.getenv("REEF_TEST_YARN")));

    final YarnConfiguration yarnConfig = new YarnConfiguration();

    // Start YARN client and register the application

    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConfig);
    yarnClient.start();

    final ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);
    containerContext.setCommands(Collections.<String>emptyList());
    containerContext.setLocalResources(Collections.<String, LocalResource>emptyMap());
    containerContext.setEnvironment(Collections.<String, String>emptyMap());
    containerContext.setTokens(getTokens());

    final ApplicationSubmissionContext appContext = yarnClient.createApplication().getApplicationSubmissionContext();
    appContext.setApplicationName("REEF_Unmanaged_AM_Test");
    appContext.setAMContainerSpec(containerContext);
    appContext.setUnmanagedAM(true);
    appContext.setQueue("default");

    final ApplicationId applicationId = appContext.getApplicationId();
    LOG.log(Level.INFO, "Registered YARN application: {0}", applicationId);

    yarnClient.submitApplication(appContext);

    LOG.log(Level.INFO, "YARN application submitted: {0}", applicationId);

    addToken(yarnClient.getAMRMToken(applicationId));

    // Start the AM

    final AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
    rmClient.init(yarnConfig);
    rmClient.start();

    final NMClientAsync nmClient = new NMClientAsyncImpl(this);
    nmClient.init(yarnConfig);
    nmClient.start();

    final RegisterApplicationMasterResponse registration =
        rmClient.registerApplicationMaster(NetUtils.getHostname(), -1, null);

    LOG.log(Level.INFO, "Unmanaged AM is running: {0}", registration);

    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Success!", null);

    LOG.log(Level.INFO, "Unregistering AM: state {0}", rmClient.getServiceState());

    // Shutdown the AM

    rmClient.stop();
    nmClient.stop();

    // Get the final application report

    final ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);
    final YarnApplicationState appState = appReport.getYarnApplicationState();
    final FinalApplicationStatus finalAttemptStatus = appReport.getFinalApplicationStatus();

    LOG.log(Level.INFO, "Application {0} final attempt {1} status: {2}/{3}", new Object[] {
        applicationId, appReport.getCurrentApplicationAttemptId(), appState, finalAttemptStatus});

    Assert.assertEquals("Application must be in FINISHED state", YarnApplicationState.FINISHED, appState);
    Assert.assertEquals("Final status must be SUCCEEDED", FinalApplicationStatus.SUCCEEDED, finalAttemptStatus);

    // Shutdown YARN client

    yarnClient.stop();
  }

  private static ByteBuffer getTokens() throws IOException {

    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final Credentials credentials = ugi.getCredentials();

    try (DataOutputBuffer dob = new DataOutputBuffer()) {
      credentials.writeTokenStorageToStream(dob);
      return ByteBuffer.wrap(dob.getData());
    }
  }

  private static void addToken(final Token<AMRMTokenIdentifier> token) throws IOException {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addToken(token);
  }

  @Override
  public void onShutdownRequest() {
    LOG.log(Level.INFO, "Shutdown requested by YARN");
  }

  // Methods below are dummy implementations of the required callbacks.

  @Override
  public void onContainersCompleted(final List<ContainerStatus> list) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onContainersAllocated(final List<Container> list) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onNodesUpdated(final List<NodeReport> list) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void onError(final Throwable throwable) {
    throw new RuntimeException("This method should never be invoked", throwable);
  }

  @Override
  public void onContainerStarted(final ContainerId containerId, final Map<String, ByteBuffer> map) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onContainerStatusReceived(final ContainerId containerId, final ContainerStatus containerStatus) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onContainerStopped(final ContainerId containerId) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onStartContainerError(final ContainerId containerId, final Throwable throwable) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onGetContainerStatusError(final ContainerId containerId, final Throwable throwable) {
    throw new RuntimeException("This method should never be invoked");
  }

  @Override
  public void onStopContainerError(final ContainerId containerId, final Throwable throwable) {
    throw new RuntimeException("This method should never be invoked");
  }
}
