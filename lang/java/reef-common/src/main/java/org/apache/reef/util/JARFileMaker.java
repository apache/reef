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

  private final FileOutputStream fileOutputStream;
  private final JarOutputStream jarOutputStream;
  private String relativeStartCanonicalPath = null;

  public JARFileMaker(final File outputFile, final Manifest manifest) throws IOException {
    this.fileOutputStream = new FileOutputStream(outputFile);
    this.jarOutputStream = new JarOutputStream(this.fileOutputStream, manifest);
  }

  public JARFileMaker(final File outputFile) throws IOException {
    this.fileOutputStream = new FileOutputStream(outputFile);
    this.jarOutputStream = new JarOutputStream(this.fileOutputStream);
  }

  /**
   * Adds a file to the JAR. If inputFile is a folder, it will be added recursively.
   *
   * @param inputFile
   * @throws IOException
   */
  public JARFileMaker add(final File inputFile) throws IOException {

    final String fileNameInJAR = makeRelative(inputFile);
    if (inputFile.isDirectory()) {
      final JarEntry entry = new JarEntry(fileNameInJAR);
      entry.setTime(inputFile.lastModified());
      this.jarOutputStream.putNextEntry(entry);
      this.jarOutputStream.closeEntry();
      final File[] files = inputFile.listFiles();
      if (files != null) {
        for (final File nestedFile : files) {
          add(nestedFile);
        }
      }
      return this;
    }

    final JarEntry entry = new JarEntry(fileNameInJAR);
    entry.setTime(inputFile.lastModified());
    this.jarOutputStream.putNextEntry(entry);
    try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(inputFile))) {
      IOUtils.copy(in, this.jarOutputStream);
      this.jarOutputStream.closeEntry();
    } catch (final FileNotFoundException ex) {
      LOG.log(Level.WARNING, "Skip the file: " + inputFile, ex);
    }
    return this;
  }

  public JARFileMaker addChildren(final File folder) throws IOException {
    this.relativeStartCanonicalPath = folder.getCanonicalPath();
    final File[] files = folder.listFiles();
    if (files != null) {
      for (final File f : files) {
        this.add(f);
      }
    }
    this.relativeStartCanonicalPath = null;
    return this;
  }

  private String makeRelative(final File input) throws IOException {
    final String result;
    if (this.relativeStartCanonicalPath == null) {
      result = input.getCanonicalPath();
    } else {
      result = input.getCanonicalPath()
          .replace(this.relativeStartCanonicalPath, "") // Drop the absolute prefix
          .substring(1);                                // drop the '/' at the beginning
    }
    if (input.isDirectory()) {
      return result.replace("\\", "/") + "/";
    } else {
      return result.replace("\\", "/");
    }

  }

  @Override
  public void close() throws IOException {
    this.jarOutputStream.close();
    this.fileOutputStream.close();
  }
}
