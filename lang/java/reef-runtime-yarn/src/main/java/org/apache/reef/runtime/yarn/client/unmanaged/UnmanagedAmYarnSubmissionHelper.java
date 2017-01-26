/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.reef.runtime.yarn.client.SecurityTokenProvider;
import org.apache.reef.runtime.yarn.client.UserCredentialSecurityTokenProvider;
import org.apache.reef.runtime.yarn.util.YarnTypes;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper code that wraps the YARN Client API for our purposes.
 */
final class UnmanagedAmYarnSubmissionHelper implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(UnmanagedAmYarnSubmissionHelper.class.getName());

  private final SecurityTokenProvider tokenProvider;
  private final YarnClient yarnClient;
  private final ApplicationSubmissionContext applicationSubmissionContext;
  private final ApplicationId applicationId;

  UnmanagedAmYarnSubmissionHelper(final YarnConfiguration yarnConfiguration,
      final SecurityTokenProvider tokenProvider) throws IOException, YarnException {

    this.tokenProvider = tokenProvider;

    LOG.log(Level.FINE, "Initializing YARN Client");
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(yarnConfiguration);
    this.yarnClient.start();
    LOG.log(Level.FINE, "Initialized YARN Client");

    LOG.log(Level.FINE, "Requesting UNMANAGED Application ID from YARN.");

    final ContainerLaunchContext launchContext = YarnTypes.getContainerLaunchContext(
        Collections.<String>emptyList(), Collections.<String, LocalResource>emptyMap(), tokenProvider.getTokens());

    final YarnClientApplication yarnClientApplication = this.yarnClient.createApplication();

    this.applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
    this.applicationSubmissionContext.setAMContainerSpec(launchContext);
    this.applicationSubmissionContext.setUnmanagedAM(true);

    this.applicationId = this.applicationSubmissionContext.getApplicationId();

    LOG.log(Level.INFO, "YARN UNMANAGED Application ID: {0}", this.applicationId);
  }

  /**
   * @return the application ID assigned by YARN.
   */
  String getStringApplicationId() {
    return this.applicationId.toString();
  }

  /**
   * Set the name of the application to be submitted.
   * @param applicationName YARN application name - a human-readable string.
   * @return reference to self for chain calls.
   */
  UnmanagedAmYarnSubmissionHelper setApplicationName(final String applicationName) {
    this.applicationSubmissionContext.setApplicationName(applicationName);
    return this;
  }

  /**
   * Set the priority of the job.
   * @param priority YARN application priority.
   * @return reference to self for chain calls.
   */
  UnmanagedAmYarnSubmissionHelper setPriority(final int priority) {
    this.applicationSubmissionContext.setPriority(Priority.newInstance(priority));
    return this;
  }

  /**
   * Assign this job submission to a queue.
   * @param queueName YARN queue name.
   * @return reference to self for chain calls.
   */
  UnmanagedAmYarnSubmissionHelper setQueue(final String queueName) {
    this.applicationSubmissionContext.setQueue(queueName);
    return this;
  }

  void submit() throws IOException, YarnException {

    LOG.log(Level.INFO, "Submitting REEF Application with UNMANAGED AM to YARN. ID: {0}", this.applicationId);
    this.yarnClient.submitApplication(this.applicationSubmissionContext);

    final Token<AMRMTokenIdentifier> token = this.yarnClient.getAMRMToken(this.applicationId);
    this.tokenProvider.addTokens(UserCredentialSecurityTokenProvider.serializeToken(token));
  }

  @Override
  public void close() {

    if (LOG.isLoggable(Level.FINER)) {
      try {
        final ApplicationReport appReport = this.yarnClient.getApplicationReport(this.applicationId);
        LOG.log(Level.FINER, "Application {0} final attempt {1} status: {2}/{3}", new Object[] {
            this.applicationId, appReport.getCurrentApplicationAttemptId(),
            appReport.getYarnApplicationState(), appReport.getFinalApplicationStatus() });
      } catch (final IOException | YarnException ex) {
        LOG.log(Level.WARNING, "Cannot get final status of Unmanaged AM app: " + this.applicationId, ex);
      }
    }

    LOG.log(Level.FINE, "Closing Unmanaged AM YARN application: {0}", this.applicationId);
    this.yarnClient.stop();
  }
}
