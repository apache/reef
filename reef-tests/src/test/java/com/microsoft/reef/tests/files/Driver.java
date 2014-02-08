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

import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.tests.exceptions.DriverSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.File;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private final Set<String> fileNamesToExpect;
  private final Clock clock;
  private final EvaluatorRequestor requestor;

  @Inject
  public Driver(@Parameter(TestDriverConfiguration.FileNamesToExpect.class) final Set<String> fileNamesToExpect,
                final Clock clock,
                final EvaluatorRequestor requestor) {
    this.fileNamesToExpect = fileNamesToExpect;
    this.clock = clock;
    this.requestor = requestor;
  }

  private void fail(final RuntimeException e) {
    // TODO: This has to go when #389 is fixed
    clock.scheduleAlarm(100, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm alarm) {
        throw e;
      }
    });
  }

  /**
   * Check that all given files are accesible.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0} Number of files in the set: {1}",
              new Object[] { startTime, Driver.this.fileNamesToExpect.size() });

      // Check whether the files made it
      for (final String fileName : Driver.this.fileNamesToExpect) {
        final File file = new File(fileName);
        LOG.log(Level.INFO, "Testing file: " + file.getAbsolutePath());
        if (!file.exists()) {
          Driver.this.fail(new DriverSideFailure("Cannot find file: " + fileName));
        } else if (!file.isFile()) {
          Driver.this.fail(new DriverSideFailure("Not a file: " + fileName));
        } else if (!file.canRead()) {
          Driver.this.fail(new DriverSideFailure("Can't read: " + fileName));
        }
      }

      // Ask for a single evaluator.
      Driver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
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
        for (final String fileName : Driver.this.fileNamesToExpect) {
          allocatedEvaluator.addFileResource(new File(fileName));
        }

        // Trivial context Configuration
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "TestContext")
            .build();

        // Filling out a TaskConfiguration
        final Configuration taskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "TestTask")
            .set(TaskConfiguration.TASK, TestTask.class)
            .build();

        // Adding the job-specific Configuration
        ConfigurationModule testTaskConfigurationModule = TestTaskConfiguration.CONF;
        for (final String fileName : Driver.this.fileNamesToExpect) {
          testTaskConfigurationModule =
              testTaskConfigurationModule.set(TestTaskConfiguration.EXPECTED_FILE_NAME, fileName);
        }

        // Submit the context and the task config.
        final Configuration finalTaskConfiguration =
            FileResourceTest.merge(taskConfiguration, testTaskConfigurationModule.build());

        allocatedEvaluator.submitContextAndTask(contextConfiguration, finalTaskConfiguration);

      } catch (final Exception e) {
        // This fails the test.
        throw new DriverSideFailure("Unable to submit context and task", e);
      }
    }
  }
}
