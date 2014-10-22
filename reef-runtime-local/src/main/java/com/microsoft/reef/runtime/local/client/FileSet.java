/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.runtime.local.client;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.OptionalParameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages a set of files.
 */
final class FileSet {
  private static final Logger LOG = Logger.getLogger(FileSet.class.getName());
  private final Set<File> theFiles = new HashSet<>();
  private final Set<String> fileNames = new HashSet<>();

  /**
   * Add a file to the FileSet.
   * <p/>
   * If the file is a directory, it is turned into a JAR and the resulting JAR is added.
   * <p/>
   * Files already added will be ignored.
   *
   * @param file the file to be added.
   */
  final void add(final File file) {
    if (file.isFile()) {
      if (this.fileNames.contains(file.getName())) {
        LOG.log(Level.FINEST, "A file with this name has already been added: {0}", file.getName());
      } else {
        this.fileNames.add(file.getName());
        this.theFiles.add(file);
      }
    } else {
      LOG.log(Level.FINEST, "Ignoring, because it is not a proper file: {0}", file);
    }
  }

  final boolean containsFileWithName(final String name) {
    return this.fileNames.contains(name);
  }

  /**
   * @return an iterable over the filenames, sans the folder. e.g. "/tmp/foo.txt" is returned as "foo.txt"
   */
  final Set<String> fileNames() {
    return this.fileNames;
  }

  /**
   * Copies all files in the current FileSet to the given destinationFolder.
   *
   * @param destinationFolder the folder where the files shall be copied to.
   * @throws IOException
   */
  final void copyTo(final File destinationFolder) throws IOException {
    for (final File f : this.theFiles) {
      final File destinationFile = new File(destinationFolder, f.getName());
      Files.copy(f.toPath(), destinationFile.toPath());
    }
  }

  /**
   * Creates symbolic links for the current FileSet into the given destinationFolder.
   *
   * @param destinationFolder the folder where the symbolic links will be created.
   * @throws IOException
   */
  final void createSymbolicLinkTo(final File destinationFolder) throws IOException {
    for (final File f : this.theFiles) {
      final File destinationFile = new File(destinationFolder, f.getName());
      Files.createSymbolicLink(destinationFile.toPath(), f.toPath());
    }
  }

  /**
   * Adds the file names of this FileSet to the given field of the given ConfigurationModule.
   *
   * @param input the ConfigurationModule to fill out
   * @param field the field to add the files in this set to.
   * @return the filled out ConfigurationModule
   */
  final ConfigurationModule addNamesTo(final ConfigurationModule input, final OptionalParameter<String> field) {
    ConfigurationModule result = input;
    for (final String fileName : this.fileNames()) {
      result = result.set(field, fileName);
    }
    return result;
  }
}
