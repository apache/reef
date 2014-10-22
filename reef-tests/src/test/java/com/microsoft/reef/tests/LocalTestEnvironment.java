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
package com.microsoft.reef.tests;

import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;

/**
 * A TestEnvironment for the local resourcemanager.
 */
public final class LocalTestEnvironment extends TestEnvironmentBase implements TestEnvironment {

  // Used to make sure the tests call the methods in the right order.
  private boolean ready = false;

  /**
   * The number of threads allocated to the local runtime.
   */
  public static final int NUMBER_OF_THREADS = 4;

  @Override
  public synchronized final void setUp() {
    this.ready = true;
  }

  @Override
  public synchronized final Configuration getRuntimeConfiguration() {
    assert (this.ready);
    final String rootFolder = System.getProperty("com.microsoft.reef.runtime.local.folder");
    if (null == rootFolder) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUMBER_OF_THREADS)
          .build();
    } else {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUMBER_OF_THREADS)
          .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, rootFolder)
          .build();

    }
  }

  @Override
  public synchronized final void tearDown() {
    assert (this.ready);
    this.ready = false;
  }

  @Override
  public int getTestTimeout() {
    return 60000; // 1 min.
  }
}
