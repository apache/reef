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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.annotations.audience.Private;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * The DFS evaluator logger that performs regular append. dfs.support.append should be true.
 */
@Private
public final class DFSEvaluatorLogAppendReaderWriter implements DFSEvaluatorLogReaderWriter {

  private final FileSystem fileSystem;
  private final Path changelogPath;
  private final DFSLineReader reader;

  private boolean fsClosed = false;

  DFSEvaluatorLogAppendReaderWriter(final FileSystem fileSystem, final Path changelogPath) {
    this.fileSystem = fileSystem;
    this.changelogPath = changelogPath;
    this.reader = new DFSLineReader(fileSystem);
  }

  /**
   * Writes a formatted entry (addition or removal) for an Evaluator ID into the DFS evaluator log.
   * The entry is appended regularly by an FS that supports append.
   * @param formattedEntry The formatted entry (entry with evaluator ID and addition/removal information).
   * @throws IOException
   */
  @Override
  public synchronized void writeToEvaluatorLog(final String formattedEntry) throws IOException {
    final boolean fileCreated = this.fileSystem.exists(this.changelogPath);

    try (
        final BufferedWriter bw = fileCreated ?
            new BufferedWriter(new OutputStreamWriter(
                this.fileSystem.append(this.changelogPath), StandardCharsets.UTF_8)) :
            new BufferedWriter(new OutputStreamWriter(
                this.fileSystem.create(this.changelogPath), StandardCharsets.UTF_8))
    ) {
      bw.write(formattedEntry);
    }
  }

  @Override
  public Iterable<String> readFromEvaluatorLog() throws IOException {
    return reader.readLinesFromFile(changelogPath);
  }

  /**
   * Closes the FileSystem.
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    if (this.fileSystem != null && !this.fsClosed) {
      this.fileSystem.close();
      this.fsClosed = true;
    }
  }
}
