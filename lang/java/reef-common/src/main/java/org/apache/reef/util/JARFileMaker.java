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
package org.apache.reef.util;

import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to create JAR files.
 */
public class JARFileMaker implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(JARFileMaker.class.getName());

  private final JarOutputStream jarOutputStream;

  public JARFileMaker(final File outputFile) throws IOException {
    this(outputFile, null);
  }

  public JARFileMaker(final File outputFile, final Manifest manifest) throws IOException {
    LOG.log(Level.FINER, "Output jar: {0}", outputFile);
    final FileOutputStream outputStream = new FileOutputStream(outputFile);
    this.jarOutputStream = manifest == null ?
        new JarOutputStream(outputStream) : new JarOutputStream(outputStream, manifest);
  }

  /**
   * Adds a file to the JAR. If inputFile is a folder, it will be added recursively.
   * @param inputFile file or directory to be added to the jar.
   * @throws IOException if cannot create a jar.
   */
  public JARFileMaker add(final File inputFile) throws IOException {
    return this.add(inputFile, null);
  }

  public JARFileMaker addChildren(final File folder) throws IOException {
    LOG.log(Level.FINEST, "Add children: {0}", folder);
    for (final File nestedFile : CollectionUtils.nullToEmpty(folder.listFiles())) {
      this.add(nestedFile);
    }
    return this;
  }

  private JARFileMaker add(final File inputFile, final String prefix) throws IOException {

    final String fileNameInJAR = createPathInJar(inputFile, prefix);
    LOG.log(Level.FINEST, "Add {0} as {1}", new Object[] {inputFile, fileNameInJAR});

    final JarEntry entry = new JarEntry(fileNameInJAR);
    entry.setTime(inputFile.lastModified());
    this.jarOutputStream.putNextEntry(entry);

    if (inputFile.isDirectory()) {
      this.jarOutputStream.closeEntry();
      for (final File nestedFile : CollectionUtils.nullToEmpty(inputFile.listFiles())) {
        this.add(nestedFile, fileNameInJAR);
      }
    } else {
      try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(inputFile))) {
        IOUtils.copy(in, this.jarOutputStream);
      } catch (final FileNotFoundException ex) {
        LOG.log(Level.WARNING, "Skip the file: " + inputFile, ex);
      } finally {
        this.jarOutputStream.closeEntry();
      }
    }

    return this;
  }

  private static String createPathInJar(final File inputFile, final String prefix) {
    final StringBuilder buf = new StringBuilder();
    if (prefix != null) {
      buf.append(prefix);
    }
    buf.append(inputFile.getName());
    if (inputFile.isDirectory()) {
      buf.append('/');
    }
    return buf.toString();
  }

  @Override
  public void close() throws IOException {
    this.jarOutputStream.close();
  }
}
