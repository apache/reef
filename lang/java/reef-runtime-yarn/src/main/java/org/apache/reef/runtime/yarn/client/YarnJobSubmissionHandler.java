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
package org.apache.reef.runtime.yarn.client;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverIsUnmanaged;
import org.apache.reef.driver.parameters.DriverJobSubmissionDirectory;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.runtime.yarn.client.unmanaged.YarnProxyUser;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());

  private final String defaultQueueName;
  private final boolean isUnmanaged;
  private final YarnConfiguration yarnConfiguration;
  private final JobJarMaker jobJarMaker;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final JobUploader uploader;
  private final YarnProxyUser yarnProxyUser;
  private final SecurityTokenProvider tokenProvider;
  private final DriverConfigurationProvider driverConfigurationProvider;

  private String applicationId;

  @Inject
  YarnJobSubmissionHandler(
          @Parameter(JobQueue.class) final String defaultQueueName,
          @Parameter(DriverIsUnmanaged.class) final boolean isUnmanaged,
          final YarnConfiguration yarnConfiguration,
          final JobJarMaker jobJarMaker,
          final REEFFileNames fileNames,
          final ClasspathProvider classpath,
          final JobUploader uploader,
          final YarnProxyUser yarnProxyUser,
          final SecurityTokenProvider tokenProvider,
          final DriverConfigurationProvider driverConfigurationProvider) throws IOException {

    this.defaultQueueName = defaultQueueName;
    this.isUnmanaged = isUnmanaged;
    this.yarnConfiguration = yarnConfiguration;
    this.jobJarMaker = jobJarMaker;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.uploader = uploader;
    this.yarnProxyUser = yarnProxyUser;
    this.tokenProvider = tokenProvider;
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    final String id = jobSubmissionEvent.getIdentifier();
    LOG.log(Level.FINEST, "Submitting{0} job: {1}",
        new Object[] {this.isUnmanaged ? " UNMANAGED AM" : "", jobSubmissionEvent});

    try (YarnSubmissionHelper submissionHelper = new YarnSubmissionHelper(this.yarnConfiguration,
        this.fileNames, this.classpath, this.yarnProxyUser, this.tokenProvider, this.isUnmanaged)) {

      LOG.log(Level.FINE, "Assembling submission JAR for the Driver.");
      final Optional<String> userBoundJobSubmissionDirectory =
          getUserBoundJobSubmissionDirectory(jobSubmissionEvent.getConfiguration());
      final JobFolder jobFolderOnDfs = userBoundJobSubmissionDirectory.isPresent()
          ? this.uploader.createJobFolder(userBoundJobSubmissionDirectory.get())
          : this.uploader.createJobFolder(submissionHelper.getApplicationId());
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, jobFolderOnDfs.getPath());
      final File jobSubmissionFile = this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);
      final LocalResource driverJarOnDfs =
          jobFolderOnDfs.uploadAsLocalResource(jobSubmissionFile, LocalResourceType.ARCHIVE);

      submissionHelper
          .addLocalResource(this.fileNames.getREEFFolderName(), driverJarOnDfs)
          .setApplicationName(id)
          .setDriverResources(jobSubmissionEvent.getDriverMemory().get(), jobSubmissionEvent.getDriverCPUCores().get())
          .setPriority(getPriority(jobSubmissionEvent))
          .setQueue(getQueue(jobSubmissionEvent))
          .setPreserveEvaluators(getPreserveEvaluators(jobSubmissionEvent))
          .setMaxApplicationAttempts(getMaxApplicationSubmissions(jobSubmissionEvent))
          .submit();

      this.applicationId = submissionHelper.getStringApplicationId();
      LOG.log(Level.FINEST, "Submitted{0} job with ID {1} :: {2}", new String[] {
          this.isUnmanaged ? " UNMANAGED AM" : "", id, this.applicationId});

    } catch (final YarnException | IOException e) {
      throw new RuntimeException("Unable to submit Driver to YARN: " + id, e);
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
   * Assembles the Driver configuration.
   */
  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final Path jobFolderPath) throws IOException {

    return this.driverConfigurationProvider.getDriverConfiguration(
            jobFolderPath.toUri(),
            jobSubmissionEvent.getRemoteId(),
            jobSubmissionEvent.getIdentifier(),
            jobSubmissionEvent.getConfiguration());
  }

  private static int getPriority(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getPriority().orElse(0);
  }

  /**
   * Extract the queue name from the jobSubmissionEvent or return default if none is set.
   */
  private String getQueue(final JobSubmissionEvent jobSubmissionEvent) {
    return getQueue(jobSubmissionEvent.getConfiguration());
  }

  /**
   * Extract the information on whether or not the job should preserve evaluators across job driver restarts.
   */
  private Boolean getPreserveEvaluators(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getPreserveEvaluators().orElse(false);
  }

  /**
   * Extract the number of maximum application attempts on the job.
   */
  private Integer getMaxApplicationSubmissions(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getMaxApplicationSubmissions().orElse(1);
  }

  /**
   * Extracts the queue name from the driverConfiguration or return default if none is set.
   *
   * @param driverConfiguration The drievr configuration
   * @return the queue name from the driverConfiguration or return default if none is set.
   */
  private String getQueue(final Configuration driverConfiguration) {
    try {
      return Tang.Factory.getTang().newInjector(driverConfiguration).getNamedInstance(JobQueue.class);
    } catch (final InjectionException e) {
      return this.defaultQueueName;
    }
  }

  private static Optional<String> getUserBoundJobSubmissionDirectory(final Configuration configuration) {
    try {
      return Optional.ofNullable(Tang.Factory.getTang().newInjector(configuration)
          .getNamedInstance(DriverJobSubmissionDirectory.class));
    } catch (final InjectionException ex) {
      return Optional.empty();
    }

  }

}
