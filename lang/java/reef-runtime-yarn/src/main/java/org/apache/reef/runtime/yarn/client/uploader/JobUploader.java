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
package org.apache.reef.runtime.yarn.client.uploader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.runtime.yarn.driver.JobSubmissionDirectoryProvider;
import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to upload the driver files to HDFS.
 */
public final class JobUploader {

  private static final Logger LOG = Logger.getLogger(JobUploader.class.getName());

  private final FileSystem fileSystem;
  private final JobSubmissionDirectoryProvider jobSubmissionDirectoryProvider;

  @Inject
  JobUploader(final YarnConfiguration yarnConfiguration,
              final JobSubmissionDirectoryProvider jobSubmissionDirectoryProvider) throws IOException {
    this.jobSubmissionDirectoryProvider = jobSubmissionDirectoryProvider;
    this.fileSystem = FileSystem.get(yarnConfiguration);
  }

  /**
   * Creates the Job folder on the DFS.
   *
   * @param applicationId
   * @return a reference to the JobFolder that can be used to upload files to it.
   * @throws IOException
   */
  public JobFolder createJobFolderWithApplicationId(final String applicationId) throws IOException {
    final Path jobFolderPath = jobSubmissionDirectoryProvider.getJobSubmissionDirectoryPath(applicationId);
    final String finalJobFolderPath = jobFolderPath.toString();
    LOG.log(Level.FINE, "Final job submission Directory: " + finalJobFolderPath);
    return createJobFolder(finalJobFolderPath);
  }


  /**
   * Convenience override for int ids.
   *
   * @param finalJobFolderPath
   * @return
   * @throws IOException
   */
  public JobFolder createJobFolder(final String finalJobFolderPath) throws IOException {
    LOG.log(Level.FINE, "Final job submission Directory: " + finalJobFolderPath);
    return new JobFolder(this.fileSystem, new Path(finalJobFolderPath));
  }

  /**
   * Convenience override for int ids.
   *
   * @param applicationId
   * @return
   * @throws IOException
   */
  public JobFolder createJobFolder(final int applicationId) throws IOException {
    return this.createJobFolderWithApplicationId(Integer.toString(applicationId));
  }
}
