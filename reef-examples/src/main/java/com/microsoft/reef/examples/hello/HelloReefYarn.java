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
package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example.
 */
public final class HelloReefYarn {

  private static final Logger LOG = Logger.getLogger(HelloReefYarn.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 30000; // 30 sec.


  /**
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, HelloReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile())
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Start Hello REEF job. Runs method runHelloReef().
   *
   * @param args command line parameters.
   * @throws com.microsoft.tang.exceptions.BindException      configuration error.
   * @throws com.microsoft.tang.exceptions.InjectionException configuration error.
   */
  public static void main(final String[] args) throws InjectionException {

    final LauncherStatus status = DriverLauncher
        .getLauncher(YarnClientConfiguration.CONF.build())
        .run(getDriverConfiguration(), JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
