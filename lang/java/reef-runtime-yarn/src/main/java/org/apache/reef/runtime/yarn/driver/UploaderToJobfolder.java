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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Uploads files to the current job folder.
 */
final class UploaderToJobFolder {
  private static final Logger LOG = Logger.getLogger(UploaderToJobFolder.class.getName());

  /**
   * The path on (H)DFS which is used as the job's folder.
   */
  private final String jobSubmissionDirectory;
  /**
   * The FileSystem instance to use for fs operations.
   */
  private final FileSystem fileSystem;

  @Inject
  UploaderToJobFolder(final @Parameter(JobSubmissionDirectory.class) String jobSubmissionDirectory,
                      final YarnConfiguration yarnConfiguration) throws IOException {
    this.jobSubmissionDirectory = jobSubmissionDirectory;
    this.fileSystem = FileSystem.get(yarnConfiguration);
  }

  /**
   * Uploads the given file to the job folder on (H)DFS.
   *
   * @param file
   * @return
   * @throws java.io.IOException
   */
  Path uploadToJobFolder(final File file) throws IOException {
    final Path source = new Path(file.getAbsolutePath());
    final Path destination = new Path(this.jobSubmissionDirectory + "/" + file.getName());
    LOG.log(Level.FINE, "Uploading {0} to {1}", new Object[]{source, destination});
    this.fileSystem.copyFromLocalFile(false, true, source, destination);
    return destination;
  }

  /**
   * Creates a LocalResource instance for the JAR file referenced by the given Path
   *
   * @param path
   * @return
   * @throws IOException
   */
  LocalResource makeLocalResourceForJarFile(final Path path) throws IOException {
    final LocalResource localResource = Records.newRecord(LocalResource.class);
    final FileStatus status = FileContext.getFileContext(this.fileSystem.getUri()).getFileStatus(path);
    localResource.setType(LocalResourceType.ARCHIVE);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(status.getPath()));
    localResource.setTimestamp(status.getModificationTime());
    localResource.setSize(status.getLen());
    return localResource;
  }
}
