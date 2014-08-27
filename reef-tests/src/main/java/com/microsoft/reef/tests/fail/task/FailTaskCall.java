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
package com.microsoft.reef.tests.fail.task;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.library.exceptions.SimulatedTaskFailure;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic task that just fails when we run it.
 */
public final class FailTaskCall implements Task {

  private static final Logger LOG = Logger.getLogger(FailTaskCall.class.getName());

  @Inject
  public FailTaskCall() {
    LOG.info("FailTaskCall created.");
  }

  @Override
  public byte[] call(final byte[] memento) throws SimulatedTaskFailure {
    final SimulatedTaskFailure ex = new SimulatedTaskFailure("FailTaskCall.call() invoked.");
    LOG.log(Level.FINE, "FailTaskCall.call() invoked: {0}", ex);
    throw ex;
  }
}
