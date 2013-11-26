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
package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

/**
 * REEF Hello world - end-to-end test.
 */
public class HelloReefTest {

  private static final int JOB_TIMEOUT = 10000; // 10 sec.

  /**
   * Run Hello world on REEF in local mode.
   *
   * @throws BindException        configuration error.
   * @throws InjectionException   configuration error.
   * @throws InterruptedException waiting for the result interrupted.
   */
  @Test
  public void testLocalHelloReef() throws BindException, InjectionException, InterruptedException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    final LauncherStatus status = HelloREEF.runHelloReef(runtimeConfiguration, JOB_TIMEOUT);
    Assert.assertTrue("HelloCLR failed: " + status, status.isSuccess());
  }
}
