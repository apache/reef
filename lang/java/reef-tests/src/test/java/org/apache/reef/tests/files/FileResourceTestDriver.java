/**
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
package org.apache.reef.tests.files;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.File;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class FileResourceTestDriver {

  private static final Logger LOG = Logger.getLogger(FileResourceTestDriver.class.getName());

  private final Set<String> fileNamesToExpect;
  private final EvaluatorRequestor requestor;
  private final REEFFileNames fileNames;
  private final File localFolder;

  @Inject
  public FileResourceTestDriver(@Parameter(FileResourceTestDriverConfiguration.FileNamesToExpect.class) final Set<String> fileNamesToExpect,
                                final EvaluatorRequestor requestor,
                                final REEFFileNames fileNames) {
    this.fileNamesToExpect = fileNamesToExpect;
    this.requestor = requestor;
    this.fileNames = fileNames;
    this.localFolder = fileNames.getLocalFolder();
  }

  /**
   * Check that all given files are accesible.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0} Number of files in the set: {1}",
          new Object[]{startTime, FileResourceTestDriver.this.fileNamesToExpect.size()});

      // Check whether the files made it
      for (final String fileName : FileResourceTestDriver.this.fileNamesToExpect) {
        final File file = new File(localFolder, fileName);
        LOG.log(Level.INFO, "Testing file: " + file.getAbsolutePath());
        if (!file.exists()) {
          throw new DriverSideFailure("Cannot find file: " + fileName);
        } else if (!file.isFile()) {
          throw new DriverSideFailure("Not a file: " + fileName);
        } else if (!file.canRead()) {
          throw new DriverSideFailure("Can't read: " + fileName);
        }
      }

      // Ask for a single evaluator.
      FileResourceTestDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(64).setNumberOfCores(1).build());
    }
  }

  /**
   * Copy files to the Evaluator and submit a Task that checks that they made it.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      try {
        // Add the files to the Evaluator.
        for (final String fileName : FileResourceTestDriver.this.fileNamesToExpect) {
          allocatedEvaluator.addFile(new File(localFolder, fileName));
        }

        // Filling out a TaskConfiguration
        final Configuration taskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "TestTask")
            .set(TaskConfiguration.TASK, FileResourceTestTask.class)
            .build();

        // Adding the job-specific Configuration
        ConfigurationModule testTaskConfigurationModule = FileResourceTestTaskConfiguration.CONF;
        for (final String fileName : FileResourceTestDriver.this.fileNamesToExpect) {
          testTaskConfigurationModule =
              testTaskConfigurationModule.set(FileResourceTestTaskConfiguration.EXPECTED_FILE_NAME, fileName);
        }

        // Submit the context and the task config.
        final Configuration finalTaskConfiguration =
            Configurations.merge(taskConfiguration, testTaskConfigurationModule.build());

        allocatedEvaluator.submitTask(finalTaskConfiguration);

      } catch (final Exception e) {
        // This fails the test.
        throw new DriverSideFailure("Unable to submit context and task", e);
      }
    }
  }
}
