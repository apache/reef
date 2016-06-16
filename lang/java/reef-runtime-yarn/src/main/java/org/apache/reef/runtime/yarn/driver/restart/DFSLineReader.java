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
import org.apache.reef.util.CloseableIterable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A reads lines from a {@link Path} on a {@link FileSystem}.
 * Assumes the file is encoded in {@link StandardCharsets#UTF_8}.
 */
final class DFSLineReader {

  private final FileSystem fileSystem;

  DFSLineReader(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  /**
   * Reads lines from the specified path.
   */
  CloseableIterable<String> readLinesFromFile(final Path path) {
    return new DFSLineReaderIterable(fileSystem, path);
  }

  /**
   * Iterable of DFS file lines.
   */
  private final class DFSLineReaderIterable implements CloseableIterable<String> {

    private final DFSLineReaderIterator iterator;

    private DFSLineReaderIterable(final FileSystem fileSystem, final Path path) {
      this.iterator = new DFSLineReaderIterator(fileSystem, path);
    }

    @Override
    public Iterator<String> iterator() {
      return iterator;
    }

    @Override
    public void close() throws Exception {
      iterator.close();
    }
  }

  /**
   * Iterator of DFS file lines.
   */
  private final class DFSLineReaderIterator implements Iterator<String>, AutoCloseable {
    private final Path path;

    private String line = null;
    private BufferedReader reader = null;

    private DFSLineReaderIterator(final FileSystem fileSystem, final Path path) {
      this.path = path;
      try {
        if (fileSystem.exists(path)) {
          // Initialize reader and read the first line if the file exists.
          // Allows hasNext and next to return true and the first line, respectively.
          // If not, reader and line simply remain null, and hasNext will return false.
          this.reader = new BufferedReader(
              new InputStreamReader(fileSystem.open(path), StandardCharsets.UTF_8));
          this.line = reader.readLine();
        }
      } catch (final IOException e) {
        throw new RuntimeException("Unable to create a reader for file " + path + ".", e);
      }
    }

    @Override
    public synchronized boolean hasNext() {
      return reader != null && line != null;
    }

    @Override
    public synchronized String next() {
      if (!hasNext()) {
        throw new NoSuchElementException("Unable to retrieve line from file " + path + ".");
      }

      // Record the line we are currently at to return, and fetch the next line.
      final String retLine = line;
      try {
        line = reader.readLine();
        if (line == null) {
          reader.close();
          reader = null;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Error retrieving next line from " + path + ".", e);
      }

      return retLine;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove is not supported.");
    }

    @Override
    public synchronized void close() throws Exception {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
