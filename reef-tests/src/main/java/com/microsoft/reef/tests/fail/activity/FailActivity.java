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

package com.microsoft.reef.tests.fail.activity;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.tests.exceptions.ActivitySideFailure;
import com.microsoft.reef.tests.exceptions.SimulatedActivityFailure;

import java.util.logging.Logger;
import java.util.logging.Level;
import javax.inject.Inject;

/**
 * A basic activity that just fails when we create it.
 */
public final class FailActivity implements Activity {

  private static final Logger LOG = Logger.getLogger(FailActivity.class.getName());

  @Inject
  public FailActivity() throws SimulatedActivityFailure {
    final SimulatedActivityFailure ex = new SimulatedActivityFailure("FailActivity constructor called.");
    LOG.log(Level.WARNING, "FailActivity created - failing now.", ex);
    throw ex;
  }

  @Override
  public byte[] call(final byte[] memento) throws ActivitySideFailure {
    final RuntimeException ex = new ActivitySideFailure("FailActivity.call() should never be called.");
    LOG.log(Level.SEVERE, "FailActivity.call() invoked - that should never happen!", ex);
    throw ex;
  }
}
