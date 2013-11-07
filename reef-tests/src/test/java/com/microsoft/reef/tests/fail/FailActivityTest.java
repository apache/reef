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
package com.microsoft.reef.tests.fail;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.TestUtils;
import com.microsoft.reef.tests.exceptions.SimulatedActivityFailure;
import com.microsoft.reef.tests.fail.activity.*;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Run Driver with different types of failures in the Activity.
 */
public final class FailActivityTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private void failOn(final Class<? extends Activity> failActivityClass) throws BindException, InjectionException {
    TestUtils.assertLauncherFailure(
        Client.run(failActivityClass,
            this.testEnvironment.getRuntimeConfiguration(), this.testEnvironment.getTestTimeout()),
        SimulatedActivityFailure.class);
  }

  @Test
  public void testFailActivity() throws BindException, InjectionException {
    failOn(FailActivity.class);
  }

  @Test
  public void testFailActivityCall() throws BindException, InjectionException {
    failOn(FailActivityCall.class);
  }

  @Test
  public void testFailActivityMsg() throws BindException, InjectionException {
    failOn(FailActivityMsg.class);
  }

  @Test
  public void testFailActivitySuspend() throws BindException, InjectionException {
    failOn(FailActivitySuspend.class);
  }

  @Test
  public void testFailActivityStart() throws BindException, InjectionException {
    failOn(FailActivityStart.class);
  }

  @Test
  public void testFailActivityStop() throws BindException, InjectionException {
    failOn(FailActivityStop.class);
  }
}
