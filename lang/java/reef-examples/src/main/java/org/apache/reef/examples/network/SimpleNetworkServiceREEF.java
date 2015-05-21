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
package org.apache.reef.examples.network;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.NetworkServiceBaseConfiguration;
import org.apache.reef.io.network.NetworkServiceDriverConfiguration;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple reef application using NetworkService
 */
public final class SimpleNetworkServiceREEF {
  private static final Logger LOG = Logger.getLogger(SimpleNetworkServiceREEF.class.getName());

  public static Configuration getDriverConfiguration() {
    Configuration driverConf =  DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SimpleNetworkServiceDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "SimpleNSExampleREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, SimpleNetworkServiceDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SimpleNetworkServiceDriver.EvaluatorAllocatedHandler.class)
        .build();

    Configuration networkConf = NetworkServiceDriverConfiguration.CONF
        .set(NetworkServiceBaseConfiguration.NETWORK_EVENTS, String.class.getName())
        .set(NetworkServiceBaseConfiguration.NETWORK_EVENTS, IntegerEvent.class.getName())
        .set(NetworkServiceBaseConfiguration.NETWORK_CODECS, StringCodec.class)
        .set(NetworkServiceBaseConfiguration.NETWORK_EVENT_HANDLERS, DriverStringEventHandler.class)
        .set(NetworkServiceDriverConfiguration.NETWORK_SERVICE_ID, "DriverNSId")
        .build();

    return Configurations.merge(driverConf, networkConf);
  }

  public static void main(final String[] args) throws BindException, InjectionException, IOException {
    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(Local.class)
        .registerShortNameOfClass(TimeOut.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 1000;

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running network service example on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
          .build();
    } else {
      LOG.log(Level.INFO, "Running network service example on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    final Configuration driverConf = getDriverConfiguration();
    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration).run(driverConf, jobTimeout);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Command line parameter == true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of seconds before timeout", short_name = "timeout", default_value = "30")
  public static final class TimeOut implements Name<Integer> {
  }
}
