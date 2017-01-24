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
package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example.
 */
public final class HelloREEF {

  private static final Logger LOG = Logger.getLogger(HelloREEF.class.getName());

  /** Number of milliseconds to wait for the job to complete. */
  private static final int JOB_TIMEOUT = 10000; // 10 sec.


  /** Configuration of the runtime. */
  private static final Configuration RUNTIME_CONFIG =
      LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
          .build();

  /** Configuration of the HelloREEF driver. */
  private static final Configuration DRIVER_CONFIG =
      DriverConfiguration.CONF
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
          .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
          .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
          .build();

  /**
   * Start Hello REEF job with local runtime.
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {

    final LauncherStatus status = DriverLauncher.getLauncher(RUNTIME_CONFIG).run(DRIVER_CONFIG, JOB_TIMEOUT);

    LOG.log(Level.INFO, "REEF job completed: {0}", status);

    ThreadLogger.logThreads(LOG, Level.FINE, "Threads running at the end of HelloREEF:");
  }

  /** Empty private constructor to prohibit instantiation of utility class. */
  private HelloREEF() { }
}
