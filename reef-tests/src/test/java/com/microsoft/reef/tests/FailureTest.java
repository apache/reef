/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.tests.yarn.failure.FailureREEF;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class FailureTest {

  private static final int JOB_TIMEOUT = 2 * 60 * 1000;

  @Test
  public void testFailureRestart() throws InjectionException {

    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, FailureREEF.NUM_LOCAL_THREADS)
        .build();

    final LauncherStatus status = FailureREEF.runFailureReef(runtimeConfiguration, JOB_TIMEOUT);

    Assert.assertTrue("FailureReef failed: " + status, status.isSuccess());
  }
}
