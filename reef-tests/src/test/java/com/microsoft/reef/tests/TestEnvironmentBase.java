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

import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Abstract base class for TestEnvironments
 */
public abstract class TestEnvironmentBase implements TestEnvironment {

  @Override
  public LauncherStatus run(final Configuration driverConfiguration) {
    try {
      return DriverLauncher.getLauncher(getRuntimeConfiguration()).run(driverConfiguration, getTestTimeout());
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
