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
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.client.parameters.RootFolder;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the files added to a driver.
 */
final class UnmanagedDriverFiles {

  private static final Logger LOG = Logger.getLogger(UnmanagedDriverFiles.class.getName());

  private final String rootFolderName;
  private final REEFFileNames fileNames;

  @Inject
  private UnmanagedDriverFiles(
      @Parameter(RootFolder.class) final String rootFolderName,
      final REEFFileNames fileNames) {

    this.rootFolderName = rootFolderName;
    this.fileNames = fileNames;
  }

  public void copyGlobalsFrom(final JobSubmissionEvent jobSubmissionEvent) throws IOException {

    final File reefGlobalPath = new File(this.rootFolderName, this.fileNames.getGlobalFolderPath());
    if (!reefGlobalPath.exists() && !reefGlobalPath.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create directory: {0}", reefGlobalPath);
      throw new RuntimeException("Failed to create directory: " + reefGlobalPath);
    }

    reefGlobalPath.deleteOnExit();

    for (final FileResource fileResource : jobSubmissionEvent.getGlobalFileSet()) {

      final File sourceFile = new File(fileResource.getPath());
      final File destinationFile = new File(reefGlobalPath, sourceFile.getName());
      LOG.log(Level.FINEST, "Copy file: {0} -> {1}", new Object[] {sourceFile, destinationFile});

      try {
        Files.createSymbolicLink(destinationFile.toPath(), sourceFile.toPath());
      } catch (final IOException ex) {
        LOG.log(Level.FINER, "Can't symlink file " + sourceFile + ", copying instead.", ex);
        Files.copy(sourceFile.toPath(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }
}
