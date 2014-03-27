/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.util;

import java.io.*;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Helper class to create JAR files.
 */
public class JARFileMaker implements AutoCloseable {

  private final FileOutputStream fileOutputStream;
  private final JarOutputStream jarOutputStream;
  private String relativeStartCanonicalPath = null;

  public JARFileMaker(final File outPutFile, final Manifest manifest) throws IOException {
    this.fileOutputStream = new FileOutputStream(outPutFile);
    this.jarOutputStream = new JarOutputStream(this.fileOutputStream, manifest);
  }

  public JARFileMaker(final File outPutFile) throws IOException {
    this.fileOutputStream = new FileOutputStream(outPutFile);
    this.jarOutputStream = new JarOutputStream(this.fileOutputStream);
  }

  /**
   * Adds a file to the JAR. If inputFile is a folder, it will be added recursively.
   *
   * @param inputFile
   * @throws IOException
   */
  public void add(final File inputFile) throws IOException {

    final String fileNameInJAR = makeRelative(inputFile);
    if (inputFile.isDirectory()) {
      final JarEntry entry = new JarEntry(fileNameInJAR);
      entry.setTime(inputFile.lastModified());
      this.jarOutputStream.putNextEntry(entry);
      this.jarOutputStream.closeEntry();
      for (final File nestedFile : inputFile.listFiles())
        add(nestedFile);
      return;
    }

    final JarEntry entry = new JarEntry(fileNameInJAR);
    entry.setTime(inputFile.lastModified());
    this.jarOutputStream.putNextEntry(entry);
    try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(inputFile))) {
      final byte[] buffer = new byte[1024];
      while (true) {
        final int count = in.read(buffer);
        if (count == -1)
          break;
        this.jarOutputStream.write(buffer, 0, count);
      }
      this.jarOutputStream.closeEntry();
    }
  }

  public void addChildren(final File folder) throws IOException {
    this.relativeStartCanonicalPath = folder.getCanonicalPath();
    for (final File f : folder.listFiles()) {
      this.add(f);
    }
    this.relativeStartCanonicalPath = null;
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
