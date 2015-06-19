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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverJobSubmissionDirectory;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.util.ReflectionUtilities;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());

  private final YarnConfiguration yarnConfiguration;
  private final JobJarMaker jobJarMaker;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final ConfigurationSerializer configurationSerializer;
  private final JobUploader uploader;
  private final double jvmSlack;

  @Inject
  YarnJobSubmissionHandler(
      final YarnConfiguration yarnConfiguration,
      final JobJarMaker jobJarMaker,
      final REEFFileNames fileNames,
      final ClasspathProvider classpath,
      final ConfigurationSerializer configurationSerializer,
      final JobUploader uploader,
      @Parameter(JVMHeapSlack.class) final double jvmSlack) throws IOException {

    this.yarnConfiguration = yarnConfiguration;
    this.jobJarMaker = jobJarMaker;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.configurationSerializer = configurationSerializer;
    this.uploader = uploader;
    this.jvmSlack = jvmSlack;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    LOG.log(Level.FINEST, "Submitting job with ID [{0}]", jobSubmissionEvent.getIdentifier());

    try (final YarnSubmissionHelper submissionHelper =
             new YarnSubmissionHelper(this.yarnConfiguration, this.fileNames, this.classpath)) {

      LOG.log(Level.FINE, "Assembling submission JAR for the Driver.");
      final JobFolder jobFolderOnDfs = this.uploader.createJobFolder(submissionHelper.getApplicationId());
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, jobFolderOnDfs.getPath());
      final File jobSubmissionFile = this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);
      final LocalResource driverJarOnDfs = jobFolderOnDfs.uploadAsLocalResource(jobSubmissionFile);

      submissionHelper
          .addLocalResource(this.fileNames.getREEFFolderName(), driverJarOnDfs)
          .setApplicationName(jobSubmissionEvent.getIdentifier())
          .setDriverMemory(jobSubmissionEvent.getDriverMemory().get())
          .setPriority(getPriority(jobSubmissionEvent))
          .setQueue(getQueue(jobSubmissionEvent, "default"))
          .submit(jobSubmissionEvent.getRemoteId());

      LOG.log(Level.FINEST, "Submitted job with ID [{0}]", jobSubmissionEvent.getIdentifier());
    } catch (final YarnException | IOException e) {
      throw new RuntimeException("Unable to submit Driver to YARN.", e);
    }
  }

  /**
   * Assembles the Driver configuration.
   */
  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final Path jobFolderPath) throws IOException {
    final Configuration config = jobSubmissionEvent.getConfiguration();
    final String userBoundJobSubmissionDirectory = config.getNamedParameter((NamedParameterNode<?>) config.getClassHierarchy().getNode(ReflectionUtilities.getFullName(DriverJobSubmissionDirectory.class)));
    LOG.log(Level.FINE, "user bound job submission Directory: " + userBoundJobSubmissionDirectory);
    final String finalJobFolderPath =
        (userBoundJobSubmissionDirectory == null || userBoundJobSubmissionDirectory.isEmpty())
            ? jobFolderPath.toString() : userBoundJobSubmissionDirectory;
    return Configurations.merge(
        YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, finalJobFolderPath)
            .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobSubmissionEvent.getIdentifier())
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, jobSubmissionEvent.getRemoteId())
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
            .build(),
        config);
  }

  private static int getPriority(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getPriority().orElse(0);
  }

  /**
   * Extract the queue name from the jobSubmissionEvent or return default if none is set.
   * <p/>
   * TODO: Revisit this. We also have a named parameter for the queue in YarnClientConfiguration.
   */
  private String getQueue(final JobSubmissionEvent jobSubmissionEvent,
                          final String defaultQueue) {
    return jobSubmissionEvent.getQueue().orElse(defaultQueue);
  }
}
