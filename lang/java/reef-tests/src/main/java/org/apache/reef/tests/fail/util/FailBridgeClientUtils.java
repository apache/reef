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

package org.apache.reef.tests.fail.util;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.JavaDriverClientLauncher;
import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.driver.service.grpc.GRPCDriverServiceConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Fail bridge client utilities.
 */
@Private
@ClientSide
public final class FailBridgeClientUtils {

  private static final Tang TANG = Tang.Factory.getTang();

  /**
   * Setup the bridge service configuration.
   * @param runtimeConfiguration runtime configuration
   * @param driverClientConfiguration driver client configuration
   * @param driverClientConfigurationProto protocol arguments
   * @return bridge service configuration
   * @throws IOException
   * @throws InjectionException
   */
  public static Configuration setupDriverService(
      final Configuration runtimeConfiguration,
      final Configuration driverClientConfiguration,
      final ClientProtocol.DriverClientConfiguration driverClientConfigurationProto)
      throws IOException, InjectionException {
    ClientProtocol.DriverClientConfiguration.Builder builder =
        ClientProtocol.DriverClientConfiguration.newBuilder(driverClientConfigurationProto);
    final File driverClientConfigurationFile = File.createTempFile("driverclient", ".conf");
    // Write driver client configuration to a file
    final Injector driverClientInjector = Tang.Factory.getTang().newInjector(driverClientConfiguration);
    final ConfigurationSerializer configurationSerializer =
        driverClientInjector.getInstance(ConfigurationSerializer.class);
    configurationSerializer.toFile(driverClientConfiguration, driverClientConfigurationFile);

    final Injector runtimeInjector = TANG.newInjector(runtimeConfiguration);
    final REEFFileNames fileNames = runtimeInjector.getInstance(REEFFileNames.class);
    final ClasspathProvider classpathProvider = runtimeInjector.getInstance(ClasspathProvider.class);
    final List<String> launchCommand = new JavaLaunchCommandBuilder(JavaDriverClientLauncher.class, null)
        .setConfigurationFilePaths(
            Collections.singletonList("./" + fileNames.getLocalFolderPath() + "/" +
                driverClientConfigurationFile.getName()))
        .setJavaPath("java")
        .setClassPath(driverClientConfigurationProto.getOperatingSystem() ==
            ClientProtocol.DriverClientConfiguration.OS.WINDOWS ?
            "\"" + StringUtils.join(classpathProvider.getDriverClasspath(), ";") + "\"" :
            StringUtils.join(classpathProvider.getDriverClasspath(), ":"))
        .build();
    final String cmd = StringUtils.join(launchCommand, ' ');
    builder.setDriverClientLaunchCommand(cmd);
    builder.addLocalFiles(driverClientConfigurationFile.getAbsolutePath());

    final IDriverServiceConfigurationProvider driverServiceConfigurationProvider = TANG.newInjector(
        TANG.newConfigurationBuilder()
            .bindImplementation(IDriverServiceConfigurationProvider.class,
                GRPCDriverServiceConfigurationProvider.class)
            .build())
        .getInstance(IDriverServiceConfigurationProvider.class);
    return driverServiceConfigurationProvider.getDriverServiceConfiguration(builder.build());
  }

  private FailBridgeClientUtils() {
  }
}
