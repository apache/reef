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

import org.apache.reef.driver.parameters.*;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Build driver configuration files
 */
public class DriverConfigBuilder {
  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobClient.class.getName());

  private static final String USER_DIR = "user.dir";
  private static final String JOB_DRIVER_CONFIG_FILE = "jobDriver.config";
  private static final String HTTP_SERVER_CONFIG_FILE = "httpServer.config";
  private static final String NAME_SERVER_CONFIG_FILE = "nameServer.config";
  private static final String DRIVER_CH_FILE = "driverClassHierarchy.bin";
  private static final String REEF_BRIDGE_PROJECT_DIR = "\\reef-bridge-project";
  private static final String REEF_BRIDGE_JAVA_DIR = "\\reef-bridge-java";
  private static final String TARGET_DIR = "\\target\\classes\\";

  /**
   * Build driver config, httpServer config and Name server config files with java bindings
   * @param driverConfiguration
   */
  public static void buildDriverConfigurationFiles(final Configuration driverConfiguration) throws IOException {
    //make the classes available in the class hierarchy so that clients can bind values to the configuration
    final ClassHierarchy ns = driverConfiguration.getClassHierarchy();
    ns.getNode(JobGlobalFiles.class.getName());
    ns.getNode(JobGlobalLibraries.class.getName());
    ns.getNode(DriverMemory.class.getName());
    ns.getNode(DriverIdentifier.class.getName());
    ns.getNode(DriverJobSubmissionDirectory.class.getName());

    serializeConfigFile(new File(getConfigFileFolder(JOB_DRIVER_CONFIG_FILE)), driverConfiguration);
    serializeConfigFile(new File(getConfigFileFolder(HTTP_SERVER_CONFIG_FILE)), JobClient.getHTTPConfiguration());
    serializeConfigFile(new File(getConfigFileFolder(NAME_SERVER_CONFIG_FILE)), JobClient.getNameServerConfiguration());

    //do this at the end to ensure all nodes are in the class hierarchy
    //serializeClassHierarchy(DRIVER_CH_FILE, driverConfiguration);
    final File classHierarchyFile = new File(getConfigFileFolder(DRIVER_CH_FILE));
    ProtocolBufferClassHierarchy.serialize(classHierarchyFile, ns);
  }

  /**
   * Serialize Configuration object into a file with configFileName
   * @param configFile
   * @param conf
   */
  private static void serializeConfigFile(final File configFile, final Configuration conf) throws IOException {
    try {
      final Injector i = Tang.Factory.getTang().newInjector(Tang.Factory.getTang().newConfigurationBuilder().build());
      final ConfigurationSerializer serializer = i.getInstance(ConfigurationSerializer.class);
      serializer.toFile(conf, configFile);
    } catch (final InjectionException e) {
      throw new RuntimeException("Cannot inject ConfigurationSerializer.");
    }
  }

  /**
   * Return folder reef-bridge-project\reef-bridge-java\target\classes
   * @param fileName
   * @return
   */
  public static String getConfigFileFolder(final String fileName) {
    final String userDir = System.getProperty(USER_DIR);
    if (userDir.endsWith(REEF_BRIDGE_PROJECT_DIR)) {
      return new StringBuilder().append(userDir).append(REEF_BRIDGE_JAVA_DIR).append(TARGET_DIR).append(fileName).toString();
    }
    if (userDir.endsWith(REEF_BRIDGE_JAVA_DIR)) {
      return new StringBuilder().append(userDir).append(TARGET_DIR).append(fileName).toString();
    }
    return new StringBuilder().append(userDir).append(REEF_BRIDGE_PROJECT_DIR).append(REEF_BRIDGE_JAVA_DIR).append(TARGET_DIR).append(fileName).toString();
  }
}