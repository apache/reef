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

package com.microsoft.reef.tests.fail.task;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.exceptions.TaskSideFailure;
import com.microsoft.reef.tests.exceptions.SimulatedTaskFailure;

import java.util.logging.Logger;
import java.util.logging.Level;
import javax.inject.Inject;

/**
 * A basic task that just fails when we create it.
 */
public final class FailTask implements Task {

  private static final Logger LOG = Logger.getLogger(FailTask.class.getName());

  @Inject
  public FailTask() throws SimulatedTaskFailure {
    final SimulatedTaskFailure ex = new SimulatedTaskFailure("FailTask constructor called.");
    LOG.log(Level.WARNING, "FailTask created - failing now.", ex);
    throw ex;
  }

  @Override
  public byte[] call(final byte[] memento) throws TaskSideFailure {
    final RuntimeException ex = new TaskSideFailure("FailTask.call() should never be called.");
    LOG.log(Level.SEVERE, "FailTask.call() invoked - that should never happen!", ex);
    throw ex;
  }
}
