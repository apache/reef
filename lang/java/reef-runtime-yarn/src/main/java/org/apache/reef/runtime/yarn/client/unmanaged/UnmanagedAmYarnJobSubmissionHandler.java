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
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.yarn.client.SecurityTokenProvider;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class UnmanagedAmYarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(UnmanagedAmYarnJobSubmissionHandler.class.getName());

  private final String defaultQueueName;
  private final UnmanagedDriverFiles driverFiles;
  private final UnmanagedAmYarnSubmissionHelper submissionHelper;

  private String applicationId = null;

  @Inject
  private UnmanagedAmYarnJobSubmissionHandler(
      @Parameter(JobQueue.class) final String defaultQueueName,
      final UnmanagedDriverFiles driverFiles,
      final YarnConfiguration yarnConfiguration,
      final SecurityTokenProvider tokenProvider) throws IOException {

    this.defaultQueueName = defaultQueueName;
    this.driverFiles = driverFiles;

    try {
      this.submissionHelper = new UnmanagedAmYarnSubmissionHelper(yarnConfiguration, tokenProvider);
    } catch (final IOException | YarnException ex) {
      LOG.log(Level.SEVERE, "Cannot create YARN client", ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close() {
    this.submissionHelper.close();
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    final String id = jobSubmissionEvent.getIdentifier();
    LOG.log(Level.FINEST, "Submitting UNMANAGED AM job: {0}", jobSubmissionEvent);

    try {
      this.driverFiles.copyGlobalsFrom(jobSubmissionEvent);

      this.submissionHelper
          .setApplicationName(id)
          .setPriority(jobSubmissionEvent.getPriority().orElse(0))
          .setQueue(getQueue(jobSubmissionEvent))
          .submit();

      this.applicationId = this.submissionHelper.getStringApplicationId();
      LOG.log(Level.FINER, "Submitted UNMANAGED AM job with ID {0} :: {1}", new String[] {id, this.applicationId});

    } catch (final IOException | YarnException ex) {
      throw new RuntimeException("Unable to submit UNMANAGED Driver to YARN: " + id, ex);
    }
  }

  /**
   * Get the RM application ID.
   * Return null if the application has not been submitted yet, or was submitted unsuccessfully.
   * @return string application ID or null if no app has been submitted yet.
   */
  @Override
  public String getApplicationId() {
    return this.applicationId;
  }

  /**
   * Extract the queue name from the jobSubmissionEvent or return default if none is set.
   */
  private String getQueue(final JobSubmissionEvent jobSubmissionEvent) {
    try {
      return Tang.Factory.getTang().newInjector(
          jobSubmissionEvent.getConfiguration()).getNamedInstance(JobQueue.class);
    } catch (final InjectionException e) {
      return this.defaultQueueName;
    }
  }
}
