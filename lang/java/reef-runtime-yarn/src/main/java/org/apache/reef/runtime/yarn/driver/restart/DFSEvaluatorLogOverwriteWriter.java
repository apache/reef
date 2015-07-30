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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.reef.annotations.audience.Private;

import java.io.*;

/**
 * The DFS evaluator logger that does not support append and does append by overwrite.
 * dfs.support.append should be false.
 */
@Private
public final class DFSEvaluatorLogOverwriteWriter implements DFSEvaluatorLogWriter {

  private final FileSystem fileSystem;

  private final Path changelogPath;

  DFSEvaluatorLogOverwriteWriter(final FileSystem fileSystem, final Path changelogPath) {
    this.fileSystem = fileSystem;
    this.changelogPath = changelogPath;
  }

  /**
   * Writes a formatted entry (addition or removal) for an Evaluator ID into the DFS evaluator log.
   * The log is appended to by reading first, adding on the information, and then overwriting the entire
   * log.
   * @param formattedEntry The formatted entry (entry with evaluator ID and addition/removal information).
   * @throws IOException when file cannot be written.
   */
  @Override
  public synchronized void writeToEvaluatorLog(final String formattedEntry) throws IOException {
    final boolean fileCreated = this.fileSystem.exists(this.changelogPath);

    if (!fileCreated) {
      try (
          final BufferedWriter bw =
              new BufferedWriter(new OutputStreamWriter(this.fileSystem.create(this.changelogPath)))) {
        bw.write(formattedEntry);
      }
    } else {
      this.appendByDeleteAndCreate(formattedEntry);
    }
  }

  /**
   * For certain HDFS implementation, the append operation may not be supported (e.g., Azure blob - wasb)
   * in this case, we will emulate the append operation by reading the content, appending entry at the end,
   * then recreating the file with appended content.
   *
   * @throws java.io.IOException when the file can't be written.
   */
  private void appendByDeleteAndCreate(final String appendEntry)
      throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try (final InputStream inputStream = this.fileSystem.open(this.changelogPath)) {
      IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    final String newContent = outputStream.toString() + appendEntry;
    this.fileSystem.delete(this.changelogPath, true);

    try (final FSDataOutputStream newOutput = this.fileSystem.create(this.changelogPath);
         final InputStream newInput = new ByteArrayInputStream(newContent.getBytes())) {
      IOUtils.copyBytes(newInput, newOutput, 4096, true);
    }
  }
}
