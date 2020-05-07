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

import org.apache.hadoop.io.file.tfile.TFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * A Helper class that wraps a TFile.Reader.Scanner.Entry, assuming that it contains YARN aggregated logs.
 */
final class LogFileEntry {
  private static final Logger LOG = Logger.getLogger(LogFileEntry.class.getName());
  private final TFile.Reader.Scanner.Entry entry;


  LogFileEntry(final TFile.Reader.Scanner.Entry entry) {
    this.entry = entry;
  }

  /**
   * Writes the contents of the entry into the given outputWriter.
   *
   * @param outputWriter
   * @throws IOException
   */
  public void write(final Writer outputWriter) throws IOException {
    try (DataInputStream keyStream = entry.getKeyStream();
         DataInputStream valueStream = entry.getValueStream()) {
      outputWriter.write("Container: ");
      outputWriter.write(keyStream.readUTF());
      outputWriter.write("\n");
      this.writeFiles(valueStream, outputWriter);
    }
  }

  /**
   * Writes the logs stored in the entry as text files in folder, one per container.
   *
   * @param folder
   * @throws IOException
   */
  public void write(final File folder) throws IOException {
    try (DataInputStream keyStream = entry.getKeyStream();
         DataInputStream valueStream = entry.getValueStream()) {
      final String containerId = keyStream.readUTF();
      try (Writer outputWriter = new OutputStreamWriter(
          new FileOutputStream(new File(folder, containerId + ".txt")), StandardCharsets.UTF_8)) {
        this.writeFiles(valueStream, outputWriter);
      }
    }
  }


  /**
   * Writes the logs of the next container to the given writer. Assumes that the valueStream is suitably positioned.
   *
   * @param valueStream
   * @param outputWriter
   * @throws IOException
   */
  private void writeFiles(final DataInputStream valueStream, final Writer outputWriter) throws IOException {
    while (valueStream.available() > 0) {
      final String strFileName = valueStream.readUTF();
      final int entryLength = Integer.parseInt(valueStream.readUTF());
      outputWriter.write("=====================================================\n");
      outputWriter.write("File Name: " + strFileName + "\n");
      outputWriter.write("File Length: " + entryLength + "\n");
      outputWriter.write("-----------------------------------------------------\n");
      this.write(valueStream, outputWriter, entryLength);
      outputWriter.write("\n");
    }
  }

  /**
   * Writes the next numberOfBytes bytes from the stream to the outputWriter, assuming that the bytes are UTF-8 encoded
   * characters.
   *
   * @param stream
   * @param outputWriter
   * @param numberOfBytes
   * @throws IOException
   */
  private void write(final DataInputStream stream, final Writer outputWriter, final int numberOfBytes)
      throws IOException {
    final byte[] buf = new byte[65535];
    int lenRemaining = numberOfBytes;
    while (lenRemaining > 0) {
      final int len = stream.read(buf, 0, lenRemaining > 65535 ? 65535 : lenRemaining);
      if (len > 0) {
        outputWriter.write(new String(buf, 0, len, "UTF-8"));
        lenRemaining -= len;
      } else {
        break;
      }
    }
  }
}
