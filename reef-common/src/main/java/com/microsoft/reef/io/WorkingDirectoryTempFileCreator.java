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

import com.microsoft.reef.annotations.Provided;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * Creates temp files in a directory named "temp" within  the current working directory.
 */
@Provided
public final class WorkingDirectoryTempFileCreator implements TempFileCreator {

  private final File tempFolder;

  @Inject
  public WorkingDirectoryTempFileCreator() throws IOException {
    final File workingDirectory = new File(".");
    this.tempFolder = Files.createTempDirectory(workingDirectory.toPath(), "temp").toFile();
  }


  @Override
  public File createTempFile(final String prefix, final String suffix) throws IOException {
    return File.createTempFile(prefix, suffix, this.tempFolder);
  }

  @Override
  public File createTempDirectory(final String prefix, final FileAttribute<?> attrs) throws IOException {
    return Files.createTempDirectory(this.tempFolder.toPath(), prefix, attrs).toFile();
  }
}
