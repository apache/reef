/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tests;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;

/**
 * Environment for REEF unit tests.
 * <p>
 * The idea is to use an instance of this class to gain access
 * to a REEF resource manager environment in order to make the tests
 * portable amongst REEF runtimes (e.g. YARN, Local, ...)
 */
public interface TestEnvironment {

  /**
   * Setup the test environment. This is typically called in a method @Before the actual test.
   */
  void setUp();

  /**
   * @return a Configuration used to obtain a REEF resourcemanager for the tests.
   * E.g. the local or YARN resource manager.
   */
  Configuration getRuntimeConfiguration();

  /**
   * Cleanup the test environment. This is typically called in a method @After the actual test.
   */
  void tearDown();

  /**
   * Return test timeout in milliseconds
   * (we need longer timeouts on YARN comparing than in local mode).
   *
   * @return test timeout in milliseconds.
   */
  int getTestTimeout();

  LauncherStatus run(Configuration driverConfiguration);

  /**
   * Returns the runtimeName for the environment.
   * @return runtimeName
   */
  String getRuntimeName();
}
