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
package org.apache.reef.runtime.yarn.driver.restart;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.driver.parameters.FailDriverOnEvaluatorLogErrors;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.yarn.util.YarnUtilities;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Evaluator Preserver that uses the DFS on YARN.
 */
@DriverSide
@RuntimeAuthor
@Unstable
public final class DFSEvaluatorPreserver implements EvaluatorPreserver, AutoCloseable {
  private static final Logger LOG = Logger.getLogger(DFSEvaluatorPreserver.class.getName());

  private static final String ADD_FLAG = "+";

  private static final String REMOVE_FLAG = "-";

  private final boolean failDriverOnEvaluatorLogErrors;

  private DFSEvaluatorLogReaderWriter readerWriter;

  private Path changeLogLocation;

  private FileSystem fileSystem;

  private boolean writerClosed = false;

  @Inject
  DFSEvaluatorPreserver(@Parameter(FailDriverOnEvaluatorLogErrors.class)
                        final boolean failDriverOnEvaluatorLogErrors) {
    this(failDriverOnEvaluatorLogErrors, "/ReefApplications/" + getEvaluatorChangeLogFolderLocation());
  }

  @Inject
  private DFSEvaluatorPreserver(@Parameter(FailDriverOnEvaluatorLogErrors.class)
                                final boolean failDriverOnEvaluatorLogErrors,
                                @Parameter(JobSubmissionDirectory.class)
                                final String jobSubmissionDirectory) {

    this.failDriverOnEvaluatorLogErrors = failDriverOnEvaluatorLogErrors;

    try {
      final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
      this.fileSystem = FileSystem.get(config);
      this.changeLogLocation =
          new Path("/" + StringUtils.strip(jobSubmissionDirectory, "/") + "/evaluatorsChangesLog");

      boolean appendSupported = config.getBoolean("dfs.support.append", false);

      if (appendSupported) {
        this.readerWriter = new DFSEvaluatorLogAppendReaderWriter(this.fileSystem, this.changeLogLocation);
      } else {
        this.readerWriter = new DFSEvaluatorLogOverwriteReaderWriter(this.fileSystem, this.changeLogLocation);
      }
    } catch (final IOException e) {
      final String errMsg = "Cannot read from log file with Exception " + e +
          ", evaluators will not be recovered.";
      final String fatalMsg = "Driver was not able to instantiate FileSystem.";

      this.handleException(e, errMsg, fatalMsg);
      this.fileSystem = null;
      this.changeLogLocation = null;
      this.readerWriter = null;
    }
  }

  /**
   * @return the folder for Evaluator changelog.
   */
  private static String getEvaluatorChangeLogFolderLocation() {
    final ApplicationId appId = YarnUtilities.getApplicationId();
    if (appId != null) {
      return appId.toString();
    }

    final String jobIdentifier = EvaluatorManager.getJobIdentifier();
    if (jobIdentifier != null) {
      return jobIdentifier;
    }

    throw new RuntimeException("Could not retrieve a suitable DFS folder for preserving Evaluator changelog.");
  }

  /**
   * Recovers the set of evaluators that are alive.
   * @return
   */
  @Override
  public synchronized Set<String> recoverEvaluators() {
    final Set<String> expectedContainers = new HashSet<>();
    try {
      if (this.fileSystem == null || this.changeLogLocation == null) {
        LOG.log(Level.WARNING, "Unable to recover evaluators due to failure to instantiate FileSystem. Returning an" +
            " empty set.");
        return expectedContainers;
      }

      for (final String line : readerWriter.readFromEvaluatorLog()) {
        if (line.startsWith(ADD_FLAG)) {
          final String containerId = line.substring(ADD_FLAG.length());
          if (expectedContainers.contains(containerId)) {
            LOG.log(Level.WARNING, "Duplicated add container record found in the change log for container " +
                containerId);
          } else {
            expectedContainers.add(containerId);
          }
        } else if (line.startsWith(REMOVE_FLAG)) {
          final String containerId = line.substring(REMOVE_FLAG.length());
          if (!expectedContainers.contains(containerId)) {
            LOG.log(Level.WARNING, "Change log includes record that try to remove non-exist or duplicate " +
                "remove record for container + " + containerId);
          }
          expectedContainers.remove(containerId);
        }
      }
    } catch (final IOException e) {
      final String errMsg = "Cannot read from log file with Exception " + e +
          ", evaluators will not be recovered.";

      final String fatalMsg = "Cannot read from evaluator log.";

      this.handleException(e, errMsg, fatalMsg);
    }
    return expectedContainers;
  }

  /**
   * Adds the allocated evaluator entry to the evaluator log.
   * @param id
   */
  @Override
  public synchronized void recordAllocatedEvaluator(final String id) {
    if (this.fileSystem != null && this.changeLogLocation != null) {
      final String entry = ADD_FLAG + id + System.lineSeparator();
      this.logContainerChange(entry);
    }
  }

  /**
   * Adds the removed evaluator entry to the evaluator log.
   * @param id
   */
  @Override
  public synchronized void recordRemovedEvaluator(final String id) {
    if (this.fileSystem != null && this.changeLogLocation != null) {
      final String entry = REMOVE_FLAG + id + System.lineSeparator();
      this.logContainerChange(entry);
    }
  }

  private void logContainerChange(final String entry) {
    try {
      this.readerWriter.writeToEvaluatorLog(entry);
    } catch (final IOException e) {
      final String errorMsg = "Unable to log the change of container [" + entry +
          "] to the container log. Driver restart won't work properly.";

      final String fatalMsg = "Unable to log container change.";

      this.handleException(e, errorMsg, fatalMsg);
    }
  }

  private void handleException(final Exception e, final String errorMsg, final String fatalMsg){
    if (this.failDriverOnEvaluatorLogErrors) {
      LOG.log(Level.SEVERE, errorMsg, e);

      try {
        this.close();
      } catch (Exception e1) {
        LOG.log(Level.SEVERE, "Failed on closing resource with " + Arrays.toString(e1.getStackTrace()));
      }

      if (fatalMsg != null) {
        throw new DriverFatalRuntimeException(fatalMsg, e);
      } else {
        throw new DriverFatalRuntimeException("Driver failed on Evaluator log error.", e);
      }
    } else {
      LOG.log(Level.WARNING, errorMsg, e);
    }
  }

  /**
   * Closes the readerWriter, which in turn closes the FileSystem.
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    if (this.readerWriter != null && !this.writerClosed) {
      this.readerWriter.close();
      this.writerClosed = true;
    }
  }
}
