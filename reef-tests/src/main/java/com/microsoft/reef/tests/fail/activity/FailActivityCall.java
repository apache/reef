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
 * A basic activity that just fails when we run it.
 */
public final class FailActivityCall implements Activity {

  private static final Logger LOG = Logger.getLogger(FailActivityCall.class.getName());

  @Inject
  public FailActivityCall() {
    LOG.info("FailActivityCall created.");
  }

  @Override
  public byte[] call(final byte[] memento) throws SimulatedActivityFailure {
    final SimulatedActivityFailure ex = new SimulatedActivityFailure("FailActivityCall.call() invoked.");
    LOG.log(Level.WARNING, "FailActivityCall.call() invoked.", ex);
    throw ex;
  }
}
