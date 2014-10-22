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
package com.microsoft.reef.io;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A TempFileCreator that uses the system's temp directory.
 */
public final class SystemTempFileCreator implements TempFileCreator {
  private static final Logger LOG = Logger.getLogger(SystemTempFileCreator.class.getName());

  @Inject
  public SystemTempFileCreator() {
    LOG.log(Level.FINE, "Temporary files and folders will be created in the system temp folder.");
  }

  @Override
  public File createTempFile(final String prefix, final String suffix) throws IOException {
    final File result = File.createTempFile(prefix, suffix);
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary file: {0}", result.getAbsolutePath());
    }
    return result;
  }

  @Override
  public File createTempDirectory(final String prefix, final FileAttribute<?> attributes) throws IOException {
    final File result = Files.createTempDirectory(prefix, attributes).toFile();
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary folder: {0}", result.getAbsolutePath());
    }
    return result;
  }

  @Override
  public File createTempDirectory(final String prefix) throws IOException {
    final File result = Files.createTempDirectory(prefix).toFile();
    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Created temporary folder: {0}", result.getAbsolutePath());
    }
    return result;
  }
}
