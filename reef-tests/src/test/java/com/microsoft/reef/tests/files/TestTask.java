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
package com.microsoft.reef.tests.files;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.exceptions.TaskSideFailure;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;

import javax.inject.Inject;
import java.io.File;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Task that checks the presence of a set of files and throws TaskSideFailure if they cannot be found or read.
 */
final class TestTask implements Task {
  private final Logger LOG = Logger.getLogger(TestTask.class.getName());
  private final Set<String> expectedFileNames;
  private final Clock clock;

  @Inject
  TestTask(@Parameter(TestTaskConfiguration.FileNamesToExpect.class) final Set<String> expectedFileNames,
           final Clock clock) {
    this.expectedFileNames = expectedFileNames;
    this.clock = clock;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    for (final String fileName : expectedFileNames) {
      final File file = new File(fileName);
      LOG.log(Level.INFO, "Testing file: " + file.getAbsolutePath());
      if (!file.exists()) {
        this.fail(new TaskSideFailure("Cannot find file: " + fileName));
      } else if (!file.isFile()) {
        this.fail(new TaskSideFailure("Not a file: " + fileName));
      } else if (!file.canRead()) {
        this.fail(new TaskSideFailure("Can't read: " + fileName));
      }
    }
    // TODO: This has to go when #389 is fixed
    clock.scheduleAlarm(100, new EventHandler<Alarm>() {
      @Override
      public void onNext(Alarm alarm) {
        LOG.log(Level.INFO, "All good");
      }
    });

    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  private void fail(final RuntimeException e) {
    // TODO: This has to go when #389 is fixed
    this.clock.scheduleAlarm(100, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm alarm) {
        throw e;
      }
    });
  }
}
