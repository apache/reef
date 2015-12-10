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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.fs.Path;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Default implementation of JobSubmissionDirectoryProvider.
 * Constructs path to job submission directory based on current date and time and applicationId.
 */
public final class JobSubmissionDirectoryProviderImpl implements JobSubmissionDirectoryProvider {

  /**
   * The path on (H)DFS which is used as the job's folder.
   */
  private final String jobSubmissionDirectory;
  private final REEFFileNames fileNames;

  @Inject
  JobSubmissionDirectoryProviderImpl(@Parameter(JobSubmissionDirectoryPrefix.class)
                                     final String jobSubmissionDirectoryPrefix,
                                     final REEFFileNames fileNames) {
    this.jobSubmissionDirectory = jobSubmissionDirectoryPrefix;
    this.fileNames = fileNames;
  }

  @Override
  public Path getJobSubmissionDirectoryPath(final String applicationId) {
    return new Path(this.jobSubmissionDirectory +
        "/" +
        new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_").format(Calendar.getInstance().getTime()) +
        this.fileNames.getJobFolderPrefix() +
        applicationId +
        "/");
  }
}
