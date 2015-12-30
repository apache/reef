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
package org.apache.reef.examples.helloCLR;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.util.EnvironmentUtils;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Hello REEF example.
 */
public final class HelloCLR {

  /**
   * The name of the class hierarchy file.
   * TODO[JIRA REEF-919] delete this constant
   */
  public static final String CLASS_HIERARCHY_FILENAME = "HelloTask.bin";

  private static final Logger LOG = Logger.getLogger(HelloCLR.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 1000000; // 1000 sec.

  private static ConfigurationModule addAll(final ConfigurationModule conf,
                                            final OptionalParameter<String> param,
                                            final File folder) {
    ConfigurationModule result = conf;
    final File[] files = folder.listFiles();
    if (files != null) {
      for (final File f : files) {
        if (f.canRead() && f.exists() && f.isFile()) {
          result = result.set(param, f.getAbsolutePath());
        }
      }
    }
    return result;
  }

  public static LauncherStatus runHelloCLR(final Configuration runtimeConf, final int timeOut, final File clrFolder)
      throws BindException, InjectionException {

    final ConfigurationModule driverConf =
        addAll(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_FILES, clrFolder)
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloCLR")
            .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class);

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf.build(), timeOut);
  }

  /**
   * Start Hello REEF job. Runs method runHelloCLR().
   *
   * @param args command line parameters.
   * @throws org.apache.reef.tang.exceptions.BindException      configuration error.
   * @throws org.apache.reef.tang.exceptions.InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
        .build();

    final File dotNetFolder = new File(args[0]).getAbsoluteFile();
    final LauncherStatus status = runHelloCLR(runtimeConfiguration, JOB_TIMEOUT, dotNetFolder);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloCLR() {
  }
}
