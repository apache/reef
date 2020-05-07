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
package org.apache.reef.runtime.hdinsight.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Parse TFile's content to key value pair.
 */
final class TFileParser {
  private static final Logger LOG = Logger.getLogger(TFileParser.class.getName());
  private final FileSystem fileSystem;
  private final Configuration configuration;

  TFileParser(final Configuration conf, final FileSystem fs) {
    this.configuration = conf;
    this.fileSystem = fs;
  }

  /**
   * Parses the given file and writes its contents into the outputWriter for all logs in it.
   *
   * @param inputPath
   * @param outputWriter
   * @throws IOException
   */
  void parseOneFile(final Path inputPath, final Writer outputWriter) throws IOException {
    try (TFile.Reader.Scanner scanner = this.getScanner(inputPath)) {
      while (!scanner.atEnd()) {
        new LogFileEntry(scanner.entry()).write(outputWriter);
        scanner.advance();
      }
    }
  }

  /**
   * Parses the given file and stores the logs for each container in a file named after the container in the given.
   * outputFolder
   *
   * @param inputPath
   * @param outputFolder
   * @throws IOException
   */
  void parseOneFile(final Path inputPath, final File outputFolder) throws IOException {
    try (TFile.Reader.Scanner scanner = this.getScanner(inputPath)) {
      while (!scanner.atEnd()) {
        new LogFileEntry(scanner.entry()).write(outputFolder);
        scanner.advance();
      }
    }
  }

  /**
   * @param path
   * @return
   * @throws IOException
   */
  private TFile.Reader.Scanner getScanner(final Path path) throws IOException {
    LOG.log(Level.FINE, "Creating Scanner for path {0}", path);
    final TFile.Reader reader = new TFile.Reader(this.fileSystem.open(path),
        this.fileSystem.getFileStatus(path).getLen(),
        this.configuration);
    final TFile.Reader.Scanner scanner = reader.createScanner();
    for (int counter = 0;
         counter < 3 && !scanner.atEnd();
         counter += 1) {
      //skip VERSION, APPLICATION_ACL, and APPLICATION_OWNER
      scanner.advance();
    }
    LOG.log(Level.FINE, "Created Scanner for path {0}", path);
    return scanner;
  }
}
