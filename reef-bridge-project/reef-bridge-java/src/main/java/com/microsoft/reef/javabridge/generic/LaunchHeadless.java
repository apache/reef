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

package com.microsoft.reef.javabridge.generic;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.*;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clr Bridge example - main class.
 */
public final class LaunchHeadless {

  /**
   * This class should not be instantiated.
   */
  private LaunchHeadless() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(LaunchHeadless.class.getName());


  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param args Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws com.microsoft.tang.exceptions.BindException      if configuration commandLineInjector fails.
   * @throws com.microsoft.tang.exceptions.InjectionException if configuration commandLineInjector fails.
   * @throws java.io.IOException        error reading the configuration.
   */

  /**
   * Main method that starts the CLR Bridge from Java
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      if(args == null || args.length == 0)
      {
        throw new IllegalArgumentException("No arguments provided, at least a clrFolder should be supplied.");
      }
      final File dotNetFolder = new File(args[0]).getAbsoluteFile();

      ConfigurationModule  driverConfigModule = JobClient.getDriverConfiguration();

      ConfigurationModule  result = driverConfigModule;
      for (final File f : dotNetFolder.listFiles()) {
        if (f.canRead() && f.exists() && f.isFile()) {
            result = result.set(DriverConfiguration.GLOBAL_FILES, f.getAbsolutePath());
        }
      }

      driverConfigModule = result;
      Configuration driverConfiguration = Configurations.merge(driverConfigModule.build(), JobClient.getHTTPConfiguration());

      LOG.log(Level.INFO, "Running on YARN");

      final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();

      final REEF reef = Tang.Factory.getTang().newInjector(runtimeConfiguration).getInstance(REEFImplementation.class);
      reef.submit(driverConfiguration);

      LOG.info("Done!");
    } catch (final BindException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
