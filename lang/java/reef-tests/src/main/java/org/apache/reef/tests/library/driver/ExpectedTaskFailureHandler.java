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
package org.apache.reef.tests.library.driver;

import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.tests.library.exceptions.ExpectedTaskException;
import org.apache.reef.util.Exceptions;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * A handler for FailedTask that will throw a DriverSideFailure unless the FailedTask was triggered by an
 * ExpectedTaskException in the Task.
 */
public final class ExpectedTaskFailureHandler implements EventHandler<FailedTask> {

  @Inject
  public ExpectedTaskFailureHandler() {
  }

  /**
   * Checks whether the FailedTask was caused by a ExpectedTaskException.
   *
   * @param failedTask
   * @throws org.apache.reef.tests.library.exceptions.DriverSideFailure if the FailedTask wasn't triggered by a
   *                                                                    ExpectedTaskException
   */

  @Override
  public void onNext(final FailedTask failedTask) {
    final Optional<Throwable> reasonOptional = failedTask.getReason();
    if (!reasonOptional.isPresent()) {
      throw new DriverSideFailure("Received a FailedTask, but it did not contain an exception.");
    } else if (!(Exceptions.getUltimateCause(reasonOptional.get()) instanceof ExpectedTaskException)) {
      throw new DriverSideFailure("Received a FailedTask, but the ExpectedTaskException isn't the ultimate cause.",
          reasonOptional.get());
    }
    failedTask.getActiveContext().get().close();
  }
}
