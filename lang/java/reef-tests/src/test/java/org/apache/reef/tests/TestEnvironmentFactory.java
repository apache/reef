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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory for the TestEnvironment.
 */
public final class TestEnvironmentFactory {

  private static final Logger LOG = Logger.getLogger(TestEnvironmentFactory.class.getName());

  /**
   * If $REEF_TEST_YARN environment variable is not set or is set to false,
   * return the local test environment; otherwise, return the one for YARN.
   *
   * @return a new TestEnvironment instance.
   */
  public static TestEnvironment getNewTestEnvironment() {
    final boolean isYarn = Boolean.parseBoolean(System.getenv("REEF_TEST_YARN"));
    final boolean isMesos = Boolean.parseBoolean(System.getenv("REEF_TEST_MESOS"));
    final boolean isAzBatch = Boolean.parseBoolean(System.getenv("REEF_TEST_AZBATCH"));

    if (isYarn ? (isMesos || isAzBatch) : (isMesos && isAzBatch)) {
      throw new RuntimeException("Cannot test on two runtimes at once");
    } else if (isYarn) {
      LOG.log(Level.INFO, "Running tests on YARN");
      return new YarnTestEnvironment();
    } else if (isMesos) {
      LOG.log(Level.INFO, "Running tests on Mesos");
      return new MesosTestEnvironment();
    } else if (isAzBatch) {
      LOG.log(Level.INFO, "Running tests on Azure Batch");
      return new AzureBatchTestEnvironment();
    } else {
      LOG.log(Level.INFO, "Running tests on Local");
      return new LocalTestEnvironment();
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private TestEnvironmentFactory() {
  }
}
