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

package org.apache.reef.bridge.driver.service;

import org.apache.reef.bridge.driver.service.grpc.GRPCDriverService;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all driver service configuration provider implementations.
 */
public abstract class DriverServiceConfigurationProviderBase implements IDriverServiceConfigurationProvider {

  private static final Tang TANG = Tang.Factory.getTang();


  protected Configuration getTcpPortRangeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto) {
    JavaConfigurationBuilder configurationModuleBuilder = TANG.newConfigurationBuilder()
        .bindSetEntry(DriverConfigurationProviders.class, TcpPortConfigurationProvider.class);
    // Setup TCP constraints
    if (driverClientConfigurationProto.getTcpPortRangeBegin() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeBegin.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeBegin()));
    }
    if (driverClientConfigurationProto.getTcpPortRangeCount() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeCount.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeCount()));
    }
    if (driverClientConfigurationProto.getTcpPortRangeTryCount() > 0) {
      configurationModuleBuilder = configurationModuleBuilder
          .bindNamedParameter(TcpPortRangeCount.class,
              Integer.toString(driverClientConfigurationProto.getTcpPortRangeTryCount()));
    }
    return configurationModuleBuilder.build();
  }

  protected Configuration getDriverConfiguration(
      final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    ConfigurationModule driverServiceConfigurationModule = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, driverConfiguration.getJobid());

    // Set file dependencies
    final List<String> localLibraries = new ArrayList<>();
    localLibraries.add(EnvironmentUtils.getClassLocation(GRPCDriverService.class));
    if (driverConfiguration.getLocalLibrariesCount() > 0) {
      localLibraries.addAll(driverConfiguration.getLocalLibrariesList());
    }
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.LOCAL_LIBRARIES, localLibraries);
    if (driverConfiguration.getGlobalLibrariesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES,
              driverConfiguration.getGlobalLibrariesList());
    }
    if (driverConfiguration.getLocalFilesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.LOCAL_FILES,
              driverConfiguration.getLocalFilesList());
    }
    if (driverConfiguration.getGlobalFilesCount() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .setMultiple(DriverConfiguration.GLOBAL_FILES,
              driverConfiguration.getGlobalFilesList());
    }
    // Setup driver resources
    if (driverConfiguration.getCpuCores() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_CPU_CORES, driverConfiguration.getCpuCores());
    }
    if (driverConfiguration.getMemoryMb() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_MEMORY, driverConfiguration.getMemoryMb());
    }
    // Setup handlers
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .set(DriverConfiguration.ON_DRIVER_STARTED, DriverServiceHandlers.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, DriverServiceHandlers.StopHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverServiceHandlers.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, DriverServiceHandlers.CompletedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, DriverServiceHandlers.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DriverServiceHandlers.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, DriverServiceHandlers.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, DriverServiceHandlers.ContextFailedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DriverServiceHandlers.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, DriverServiceHandlers.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, DriverServiceHandlers.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, DriverServiceHandlers.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, DriverServiceHandlers.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverServiceHandlers.ClientMessageHandler.class)
        .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverServiceHandlers.ClientCloseHandler.class)
        .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE, DriverServiceHandlers.ClientCloseWithMessageHandler.class);
    return driverServiceConfigurationModule.build();
  }

  protected Configuration getDriverRestartConfiguration(
      final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    ConfigurationModule restartConfModule = DriverRestartConfiguration.CONF
        .set(DriverRestartConfiguration.ON_DRIVER_RESTARTED,
            DriverServiceHandlers.DriverRestartHandler.class)
        .set(DriverRestartConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE,
            DriverServiceHandlers.DriverRestartActiveContextHandler.class)
        .set(DriverRestartConfiguration.ON_DRIVER_RESTART_TASK_RUNNING,
            DriverServiceHandlers.DriverRestartRunningTaskHandler.class)
        .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
            DriverServiceHandlers.DriverRestartCompletedHandler.class)
        .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
            DriverServiceHandlers.DriverRestartFailedEvaluatorHandler.class);
    return driverConfiguration.getDriverRestartEvaluatorRecoverySeconds() > 0 ?
        restartConfModule
            .set(DriverRestartConfiguration.DRIVER_RESTART_EVALUATOR_RECOVERY_SECONDS,
                driverConfiguration.getDriverRestartEvaluatorRecoverySeconds())
            .build() :
        restartConfModule.build();
  }
}
