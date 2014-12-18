/**
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

package org.apache.reef.javabridge.generic;

import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DriverConfigCreator
 */
public class DriverConfigCreator {
  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * Number of REEF worker threads in local mode. We assume maximum 10 evaluators can be requested on local runtime
   */
  private static final int NUM_LOCAL_THREADS = 10;

  /**
   * This program is used to generate JobDriver.config, HttpConfig.config and nameServer.config. It runs when the project is build. The config files are
   * dropped at the class folder and then packed into the jar file.
   * @param args
   */
  public static void main(final String[] args) throws IOException{
    LOG.log(Level.INFO, "Entering DriverConfigCreator");

    final boolean isLocal;
    if (args.length > 0)
    {
      isLocal = Boolean.valueOf(args[0]);
    } else {
      isLocal = true;
    }

    final Configuration clientConfig = getClientConfiguration(isLocal);
    final ConfigurationModule driverConfigModule = JobClient.getDriverConfiguration();
    final Configuration driverConfig = Configurations.merge(driverConfigModule.build(), clientConfig);

    try {
      DriverConfigBuilder.buildDriverConfigurationFiles(driverConfig);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to buildDriverConfigurationFiles.", e);
      throw e;
    }
    LOG.log(Level.INFO, "Driver config files are created.");
  }

  /**
   * get ClientConfigurations
   * @param isLocal
   * @return
   */
  private static Configuration getClientConfiguration(boolean isLocal) {
    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_COMPLETED, JobClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, JobClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, JobClient.RuntimeErrorHandler.class)
        .build();

    final Configuration runtimeConfiguration;

    if (isLocal) {
      LOG.log(Level.INFO, "Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    return Tang.Factory.getTang()
        .newConfigurationBuilder(Configurations.merge(runtimeConfiguration, clientConfiguration))
        .build();
  }
}
