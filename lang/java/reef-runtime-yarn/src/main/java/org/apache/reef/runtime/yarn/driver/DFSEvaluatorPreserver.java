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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.reef.runtime.common.driver.EvaluatorPreserver;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;

import javax.inject.Inject;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Evaluator Preserver that uses the DFS on YARN.
 */
public final class DFSEvaluatorPreserver implements EvaluatorPreserver {
  private static final Logger LOG = Logger.getLogger(DFSEvaluatorPreserver.class.getName());

  private static final String ADD_FLAG = "+";

  private static final String REMOVE_FLAG = "-";

  @Inject
  private DFSEvaluatorPreserver() {
  }

  @Override
  public Set<String> recoverEvaluators() {
    return getExpectedContainersFromLogReplay();
  }

  @Override
  public void recordAllocatedEvaluator(final String id) {
    this.logContainerAddition(id);
  }

  @Override
  public void recordRemovedEvaluator(final String id) {
    this.logContainerRemoval(id);
  }

  private synchronized void writeToEvaluatorLog(final String entry) throws IOException {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    final FileSystem fs = getFileSystemInstance();
    final Path path = new Path(getChangeLogLocation());
    final boolean appendToLog = fs.exists(path);

    try (
        final BufferedWriter bw = appendToLog ?
            new BufferedWriter(new OutputStreamWriter(fs.append(path))) :
            new BufferedWriter(new OutputStreamWriter(fs.create(path)));
    ) {
      bw.write(entry);
    } catch (final IOException e) {
      if (appendToLog) {
        LOG.log(Level.FINE, "Unable to add an entry to the Evaluator log. Attempting append by delete and recreate", e);
        appendByDeleteAndCreate(fs, path, entry);
      }
    }
  }

  private FileSystem getFileSystemInstance() throws IOException {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    return FileSystem.get(config);
  }

  /**
   * For certain HDFS implementation, the append operation may not be supported (e.g., Azure blob - wasb)
   * in this case, we will emulate the append operation by reading the content, appending entry at the end,
   * then recreating the file with appended content.
   *
   * @throws java.io.IOException when the file can't be written.
   */

  private void appendByDeleteAndCreate(final FileSystem fs, final Path path, final String appendEntry)
      throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try (final InputStream inputStream = fs.open(path)) {
      IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    final String newContent = outputStream.toString() + appendEntry;
    fs.delete(path, true);

    try (final FSDataOutputStream newOutput = fs.create(path);
         final InputStream newInput = new ByteArrayInputStream(newContent.getBytes())) {
      IOUtils.copyBytes(newInput, newOutput, 4096, true);
    }

  }

  private String getChangeLogLocation() {
    return "/ReefApplications/" + EvaluatorManager.getJobIdentifier() + "/evaluatorsChangesLog";
  }

  private void logContainerAddition(final String containerId) {
    final String entry = ADD_FLAG + containerId + System.lineSeparator();
    logContainerChange(entry);
  }

  private void logContainerRemoval(final String containerId) {
    final String entry = REMOVE_FLAG + containerId + System.lineSeparator();
    logContainerChange(entry);
  }

  private void logContainerChange(final String entry) {
    try {
      writeToEvaluatorLog(entry);
    } catch (final IOException e) {
      final String errorMsg = "Unable to log the change of container [" + entry +
          "] to the container log. Driver restart won't work properly.";
      LOG.log(Level.WARNING, errorMsg, e);
      throw new RuntimeException(errorMsg);
    }
  }

  private Set<String> getExpectedContainersFromLogReplay() {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    final Set<String> expectedContainers = new HashSet<>();
    try {
      final FileSystem fs = FileSystem.get(config);
      final Path path = new Path(getChangeLogLocation());
      if (!fs.exists(path)) {
        // empty set
        return expectedContainers;
      } else {
        final BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null) {
          if (line.startsWith(ADD_FLAG)) {
            final String containerId = line.substring(ADD_FLAG.length());
            if (expectedContainers.contains(containerId)) {
              throw new RuntimeException("Duplicated add container record found in the change log for container " +
                  containerId);
            }
            expectedContainers.add(containerId);
          } else if (line.startsWith(REMOVE_FLAG)) {
            final String containerId = line.substring(REMOVE_FLAG.length());
            if (!expectedContainers.contains(containerId)) {
              throw new RuntimeException("Change log includes record that try to remove non-exist or duplicate " +
                  "remove record for container + " + containerId);
            }
            expectedContainers.remove(containerId);
          }
          line = br.readLine();
        }
        br.close();
      }
    } catch (final IOException e) {
      throw new RuntimeException("Cannot read from log file", e);
    }
    return expectedContainers;
  }
}
