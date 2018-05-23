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

package org.apache.reef.bridge.driver.service.grpc;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.service.DriverServiceConfiguration;
import org.apache.reef.bridge.driver.service.DriverServiceHandlers;
import org.apache.reef.bridge.driver.service.DriverServiceConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.io.TcpPortConfigurationProvider;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeCount;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * GRPC driver service configuration provider.
 */
@Private
public final class GRPCDriverServiceConfigurationProvider implements DriverServiceConfigurationProvider {

  @Inject
  private GRPCDriverServiceConfigurationProvider() {
  }

  @Override
  public Configuration getDriverServiceConfiguration(
      final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    Configuration driverServiceConfiguration = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.DRIVER_SERVICE_IMPL, GRPCDriverService.class)
        .set(DriverServiceConfiguration.DRIVER_IDLENESS_SOURCES, GRPCDriverService.class)
        .set(DriverServiceConfiguration.DRIVER_CLIENT_COMMAND, driverConfiguration.getDriverClientLaunchCommand())
        .build();
    return driverConfiguration.getDriverRestartEnable() ?
        Configurations.merge(
            driverServiceConfiguration,
            getDriverRestartConfiguration(driverConfiguration),
            getDriverConfiguration(driverConfiguration),
            getTcpPortRangeConfiguration(driverConfiguration)) :
        Configurations.merge(
            driverServiceConfiguration,
            getDriverConfiguration(driverConfiguration),
            getTcpPortRangeConfiguration(driverConfiguration));
  }

  private Configuration getTcpPortRangeConfiguration(
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto) {
    JavaConfigurationBuilder configurationModuleBuilder = Tang.Factory.getTang().newConfigurationBuilder()
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

  private Configuration getDriverConfiguration(
      final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    ConfigurationModule driverServiceConfigurationModule = DriverServiceConfiguration.STATIC_DRIVER_CONF_MODULE
        .set(DriverConfiguration.DRIVER_IDENTIFIER, driverConfiguration.getJobid());

    // Set file dependencies
    final List<String> localLibraries = new ArrayList<>();
    localLibraries.add(EnvironmentUtils.getClassLocation(GRPCDriverService.class));
    localLibraries.addAll(driverConfiguration.getLocalLibrariesList());
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.LOCAL_LIBRARIES, localLibraries);
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES,
            driverConfiguration.getGlobalLibrariesList());
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.LOCAL_FILES,
            driverConfiguration.getLocalFilesList());
    driverServiceConfigurationModule = driverServiceConfigurationModule
        .setMultiple(DriverConfiguration.GLOBAL_FILES,
            driverConfiguration.getGlobalFilesList());
    // Setup driver resources
    if (driverConfiguration.getCpuCores() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_CPU_CORES, driverConfiguration.getCpuCores());
    }
    if (driverConfiguration.getMemoryMb() > 0) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_MEMORY, driverConfiguration.getMemoryMb());
    }
    // Job submission directory
    if (StringUtils.isNotEmpty(driverConfiguration.getDriverJobSubmissionDirectory())) {
      driverServiceConfigurationModule = driverServiceConfigurationModule
          .set(DriverConfiguration.DRIVER_JOB_SUBMISSION_DIRECTORY,
              driverConfiguration.getDriverJobSubmissionDirectory());
    }
    return !driverConfiguration.getEnableHttpDriver() ? driverServiceConfigurationModule.build() :
        Configurations.merge(DriverServiceConfiguration.HTTP_AND_NAMESERVER, driverServiceConfigurationModule.build());
  }

  private Configuration getDriverRestartConfiguration(
      final ClientProtocol.DriverClientConfiguration driverConfiguration) {
    final ConfigurationModule restartConfModule = DriverRestartConfiguration.CONF
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
