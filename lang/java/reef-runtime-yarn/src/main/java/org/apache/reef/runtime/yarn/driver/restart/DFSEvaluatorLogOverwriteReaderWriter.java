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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.util.CloseableIterable;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * The DFS evaluator logger that does not support append and does append by overwrite.
 * dfs.support.append should be false.
 */
@Private
public final class DFSEvaluatorLogOverwriteReaderWriter implements DFSEvaluatorLogReaderWriter {

  private final FileSystem fileSystem;

  private final DFSLineReader reader;
  private final Path changeLogPath;
  private final Path changeLogAltPath;

  // This is the last path we will be writing to.
  private Path pathToWriteTo = null;

  private boolean fsClosed = false;

  DFSEvaluatorLogOverwriteReaderWriter(final FileSystem fileSystem, final Path changeLogPath) {
    this.fileSystem = fileSystem;
    this.changeLogPath = changeLogPath;
    this.changeLogAltPath = new Path(changeLogPath + ".alt");
    this.reader = new DFSLineReader(fileSystem);
  }

  /**
   * Writes a formatted entry (addition or removal) for an Evaluator ID into the DFS evaluator log.
   * The log is appended to by reading first, adding on the information, and then overwriting the entire log.
   * Since the {@link FileSystem} does not support appends, this {@link DFSEvaluatorLogReaderWriter}
   * uses a two-file approach, where when we write, we always overwrite the older file.
   * @param formattedEntry The formatted entry (entry with evaluator ID and addition/removal information).
   * @throws IOException when file cannot be written.
   */
  @Override
  public synchronized void writeToEvaluatorLog(final String formattedEntry) throws IOException {
    final Path writePath = getWritePath();

    // readPath is always not the writePath.
    final Path readPath = getAlternativePath(writePath);

    try (final FSDataOutputStream outputStream = this.fileSystem.create(writePath, true)) {
      InputStream inputStream = null;
      try {
        final InputStream newEntryInputStream = new ByteArrayInputStream(
            formattedEntry.getBytes(StandardCharsets.UTF_8));

        if (fileSystem.exists(readPath)) {
          inputStream = new SequenceInputStream(
              this.fileSystem.open(readPath), newEntryInputStream);
        } else {
          inputStream = newEntryInputStream;
        }

        IOUtils.copyBytes(inputStream, outputStream, 4096, true);
      } finally {
        outputStream.hsync();
        if (inputStream != null) {
          inputStream.close();
        }
      }
    }
  }

  /**
   * Since the {@link FileSystem} does not support appends, this {@link DFSEvaluatorLogReaderWriter}
   * uses a two-file approach, where when we read, we always read from the newer file.
   */
  @Override
  public synchronized CloseableIterable<String> readFromEvaluatorLog() throws IOException {
    return reader.readLinesFromFile(getLongerFile());
  }

  /**
   * Gets the alternative path. Returns one of changeLogPath and changeLogAltPath.
   */
  private synchronized Path getAlternativePath(final Path path) {
    if (path.equals(changeLogPath)) {
      return changeLogAltPath;
    }

    return changeLogPath;
  }

  /**
   * Gets the path to write to.
   */
  private synchronized Path getWritePath() throws IOException {
    if (pathToWriteTo == null) {
      // If we have not yet written before, check existence of files.
      final boolean originalExists = fileSystem.exists(changeLogPath);
      final boolean altExists = fileSystem.exists(changeLogAltPath);

      if (originalExists && altExists) {
        final FileStatus originalStatus = fileSystem.getFileStatus(changeLogPath);
        final FileStatus altStatus = fileSystem.getFileStatus(changeLogAltPath);

        // Return the shorter file.
        // TODO[JIRA REEF-1413]: This approach will not be able to work in REEF-1413.
        // TODO[JIRA REEF-1413]: Note that we cannot use last modified time because Azure blob's HDFS API only
        // TODO[JIRA REEF-1413]: supports time resolution up to a second.
        final long originalLen = originalStatus.getLen();
        final long altLen = altStatus.getLen();

        if (originalLen < altLen) {
          pathToWriteTo = changeLogPath;
        } else {
          pathToWriteTo = changeLogAltPath;
        }
      } else if (originalExists) {
        // Return the file that does not exist.
        pathToWriteTo = changeLogAltPath;
      } else {
        pathToWriteTo = changeLogPath;
      }
    }

    final Path returnPath = pathToWriteTo;
    pathToWriteTo = getAlternativePath(pathToWriteTo);

    return returnPath;
  }

  private synchronized Path getLongerFile() throws IOException {
    final boolean originalExists = fileSystem.exists(changeLogPath);
    final boolean altExists = fileSystem.exists(changeLogAltPath);

    // If both files exist, return the newest file path.
    if (originalExists && altExists) {
      final FileStatus originalStatus = fileSystem.getFileStatus(changeLogPath);
      final FileStatus altStatus = fileSystem.getFileStatus(changeLogAltPath);

      final long originalLastModTime = originalStatus.getLen();
      final long altLastModTime = altStatus.getLen();

      // Return the newer file.
      if (originalLastModTime >= altLastModTime) {
        return changeLogPath;
      }

      return changeLogAltPath;
    } else if (altExists) {
      // If only the alt file exists, return the alt file path.
      return changeLogAltPath;
    }

    // If only the original file exists or if neither exist, return the original file path.
    return changeLogPath;
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
