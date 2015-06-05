/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.reef.runtime.common.files.REEFFileNames;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Helper class to upload the driver files to HDFS.
 */
public final class JobUploader {

  private final FileSystem fileSystem;
  private final REEFFileNames fileNames;

  @Inject
  JobUploader(final YarnConfiguration yarnConfiguration,
              final REEFFileNames fileNames) throws IOException {
    this.fileNames = fileNames;
    this.fileSystem = FileSystem.get(yarnConfiguration);
  }

  /**
   * Creates the Job folder on the DFS.
   *
   * @param applicationId
   * @return a reference to the JobFolder that can be used to upload files to it.
   * @throws IOException
   */
  public JobFolder createJobFolder(final String applicationId) throws IOException {
    // TODO: This really should be configurable, but wasn't in the code I moved as part of [REEF-228]
    final Path jobFolderPath = new Path("/tmp/" + this.fileNames.getJobFolderPrefix() + applicationId + "/");
    return new JobFolder(this.fileSystem, jobFolderPath);
  }

  /**
   * Convenience override for int ids.
   *
   * @param applicationId
   * @return
   * @throws IOException
   */
  public JobFolder createJobFolder(final int applicationId) throws IOException {
    return this.createJobFolder(Integer.toString(applicationId));
  }
}
