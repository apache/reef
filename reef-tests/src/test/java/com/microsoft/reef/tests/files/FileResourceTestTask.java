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
package com.microsoft.reef.tests.files;

import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.library.exceptions.TaskSideFailure;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.time.Clock;

import javax.inject.Inject;
import java.io.File;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Task that checks the presence of a set of files and throws TaskSideFailure if they cannot be found or read.
 */
final class FileResourceTestTask implements Task {
  private final Logger LOG = Logger.getLogger(FileResourceTestTask.class.getName());
  private final Set<String> expectedFileNames;
  private final Clock clock;
  private final File localFolder;

  @Inject
  FileResourceTestTask(@Parameter(FileResourceTestTaskConfiguration.FileNamesToExpect.class) final Set<String> expectedFileNames,
                       final Clock clock,
                       final REEFFileNames fileNames) {
    this.expectedFileNames = expectedFileNames;
    this.clock = clock;
    this.localFolder = fileNames.getLocalFolder();
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    for (final String fileName : expectedFileNames) {
      final File file = new File(localFolder, fileName);
      LOG.log(Level.INFO, "Testing file: " + file.getAbsolutePath());
      if (!file.exists()) {
        throw new TaskSideFailure("Cannot find file: " + fileName);
      } else if (!file.isFile()) {
        throw new TaskSideFailure("Not a file: " + fileName);
      } else if (!file.canRead()) {
        throw new TaskSideFailure("Can't read: " + fileName);
      }
    }

    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }
}
