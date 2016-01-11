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
package org.apache.reef.io;

import org.apache.reef.annotations.Provided;
import org.apache.reef.io.parameters.TempFileRootFolder;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates temp files in a directory named "temp" within  the configured directory.
 */
@Provided
public final class ConfigurableDirectoryTempFileCreator implements TempFileCreator {
  private static final Logger LOG = Logger.getLogger(ConfigurableDirectoryTempFileCreator.class.getName());
  private final File tempFolderAsFile;
  private final Path tempFolderAsPath;

  @Inject
  ConfigurableDirectoryTempFileCreator(
      @Parameter(TempFileRootFolder.class) final String rootFolder) throws IOException {
    this.tempFolderAsFile = new File(rootFolder);
    if (!this.tempFolderAsFile.exists() && !this.tempFolderAsFile.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", this.tempFolderAsFile.getAbsolutePath());
    }
    this.tempFolderAsPath = this.tempFolderAsFile.toPath();
    LOG.log(Level.FINE, "Temporary files and folders will be created in [{0}]",
        this.tempFolderAsFile.getAbsolutePath());
  }


  @Override
  public File createTempFile(final String prefix, final String suffix) throws IOException {
    final File result = File.createTempFile(prefix, suffix, this.tempFolderAsFile);
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary file: {0}", result.getAbsolutePath());
    }
    return result;
  }

  @Override
  public File createTempDirectory(final String prefix, final FileAttribute<?> fileAttributes) throws IOException {
    final File result = Files.createTempDirectory(this.tempFolderAsPath, prefix, fileAttributes).toFile();
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary folder: {0}", result.getAbsolutePath());
    }
    return result;
  }

  @Override
  public File createTempDirectory(final String prefix) throws IOException {
    final File result = Files.createTempDirectory(this.tempFolderAsPath, prefix).toFile();
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary folder: {0}", result.getAbsolutePath());
    }
    return result;
  }
}
