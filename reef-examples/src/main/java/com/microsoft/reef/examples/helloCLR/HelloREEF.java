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
package com.microsoft.reef.examples.helloCLR;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example.
 */
public final class HelloREEF {


  public static final String CLASS_HIERARCHY_FILENAME = "activity.bin";

  private static final Logger LOG = Logger.getLogger(HelloREEF.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 10000; // 10 sec.

  public static LauncherStatus runHelloReef(final Configuration runtimeConf, final int timeOut, final String[] args)
      throws BindException, InjectionException {


    ConfigurationModule driverConf =
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
            .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class);

    for (final String fileName : args) {
      final File f = new File(fileName);
      if (!f.exists()) {
        LOG.log(Level.FINEST, "Ignoring, as it doesn't exist: " + fileName);
      } else if (f.isDirectory()) {
        LOG.log(Level.FINEST, "Ignoring, as it is a folder: " + fileName);
      } else if (!f.canRead()) {
        LOG.log(Level.FINEST, "Ignoring, as it can't be read: " + fileName);
      } else {
        driverConf = driverConf.set(DriverConfiguration.GLOBAL_FILES, fileName);
      }
    }

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf.build(), timeOut);
  }

  /**
   * Start Hello REEF job. Runs method runHelloReef().
   *
   * @param args command line parameters.
   * @throws com.microsoft.tang.exceptions.BindException
   *          configuration error.
   * @throws com.microsoft.tang.exceptions.InjectionException
   *          configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    final LauncherStatus status = runHelloReef(runtimeConfiguration, JOB_TIMEOUT, args);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
