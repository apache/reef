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

import org.junit.Assert;
import org.junit.Test;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.tests.yarn.failure.FailureREEF;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

/**
 *
 */
public class FailureTest {

  /**
   * @param args
   * @throws InjectionException
   */
  @Test
  public void testFailureRestart() throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 40)
        .build();
    final LauncherStatus status = FailureREEF.runFailureReef(runtimeConfiguration, 2 * 60 * 1000);
    Assert.assertTrue("FailureReef failed: " + status, status.isSuccess());
  }

}
