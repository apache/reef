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
package com.microsoft.reef.tests;

import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;

/**
 * A TestEnvironment for the local resourcemanager.
 */
public final class LocalTestEnvironment implements TestEnvironment {

  // Used to make sure the tests call the methods in the right order.
  private boolean ready = false;

  @Override
  public synchronized final void setUp() {
    this.ready = true;
  }

  @Override
  public synchronized final Configuration getRuntimeConfiguration() {
    assert (this.ready);
    try {
      final String rootFolder = System.getProperty("com.microsoft.reef.runtime.local.folder");
      if (null == rootFolder) {
        return LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 16)
            .build();
      } else {
        return LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 16)
            .set(LocalRuntimeConfiguration.RUNTIME_ROOT_FOLDER, rootFolder)
            .build();

      }
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized final void tearDown() {
    assert (this.ready);
    this.ready = false;
  }

  @Override
  public int getTestTimeout() {
    return 30000; // 30 seconds
  }
}
