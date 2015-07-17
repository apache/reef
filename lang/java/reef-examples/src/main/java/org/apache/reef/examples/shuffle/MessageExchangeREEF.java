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
package org.apache.reef.examples.shuffle;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.shuffle.utils.NameResolverWrapper;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class MessageExchangeREEF {

  public static final Logger LOG = Logger.getLogger(MessageExchangeREEF.class.getName());

  public static final int MAX_NUMBER_OF_EVALUATORS = 15;

  public static Configuration getRuntimeConfiguration(final boolean isLocal) {
    if (isLocal) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      return YarnClientConfiguration.CONF.build();
    }
  }

  public static Configuration getDriverConfiguration(final int taskNumber) {
    final Configuration taskNumberConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageExchangeDriver.TaskNumber.class, taskNumber + "")
        .bindImplementation(NameResolver.class, NameResolverWrapper.class)
        .build();

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MessageExchangeREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MessageExchangeDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, MessageExchangeDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MessageExchangeDriver.AllocatedHandler.class)
        .build();

    return Configurations.merge(taskNumberConf, driverConf);
  }

  public static void main(final String[] args) throws Exception {
    final CommandLine commandLine = new CommandLine();
    commandLine.registerShortNameOfClass(Local.class);
    final Injector injector = Tang.Factory.getTang().newInjector(
        commandLine.parseToConfiguration(args, Local.class, MessageExchangeDriver.TaskNumber.class));

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int taskNumber = injector.getNamedInstance(MessageExchangeDriver.TaskNumber.class);

    final LauncherStatus state = DriverLauncher.getLauncher(getRuntimeConfiguration(isLocal))
        .run(getDriverConfiguration(taskNumber), 100000);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * This class is not a utility class.
   */
  private MessageExchangeREEF() {
  }

  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }
}
