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
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
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

  public static final String DRIVER_CONFIG_FILE = "driver.config";

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
  public static void buildDriverConfigurationFiles(final Configuration driverConfiguration) {
    //make the classes available in the class hierarchy so that clients can bind values to the configuration
    final ClassHierarchy ns = driverConfiguration.getClassHierarchy();
    ns.getNode(JobGlobalFiles.class.getName());
    ns.getNode(JobGlobalLibraries.class.getName());
    ns.getNode(DriverMemory.class.getName());
    ns.getNode(DriverIdentifier.class.getName());
    ns.getNode(DriverJobSubmissionDirectory.class.getName());

    serializeConfigFile(JOB_DRIVER_CONFIG_FILE, driverConfiguration);
    serializeConfigFile(HTTP_SERVER_CONFIG_FILE, JobClient.getHTTPConfiguration());
    serializeConfigFile(NAME_SERVER_CONFIG_FILE, JobClient.getNameServerConfiguration());

    //do this at the end to ensure all nodes are in the class hierarchy
    serializeClassHierarchy(DRIVER_CH_FILE, driverConfiguration);
  }
  /**
   * Serialize the ClassHierarchy in the Configuration in to a file with classHierarchyFileName
   * @param classHierarchyFileName
   * @param conf
   */
  public static void serializeClassHierarchy(final String classHierarchyFileName, final Configuration conf) {
    final String configFileFolder = getConfigFileFolder(classHierarchyFileName);
    LOG.log(Level.INFO, "configFileFolder: " + configFileFolder);
    final File classHierarchyFile = new File(configFileFolder);
    final ClassHierarchy ns = conf.getClassHierarchy();

    try {
      ProtocolBufferClassHierarchy.serialize(classHierarchyFile, ns);
    } catch (final IOException e) {
      throw new RuntimeException("Cannot create class hierarchy file at " + classHierarchyFile.getAbsolutePath());
    }
  }

  /**
   * Serialize Configuration object into a file with configFileName
   * @param configFileName
   * @param conf
   */
  public static void serializeConfigFile(final String configFileName, final Configuration conf) {
    final String configFileFolder = getConfigFileFolder(configFileName);
    LOG.log(Level.INFO, "configFileFolder: " + configFileFolder);
    final File configFile = new File(getConfigFileFolder(configFileName));
    final File configTextFile = new File(getConfigFileFolder(configFileName) + ".txt");

    try {
      //Serialize the Configuration into a file
      new AvroConfigurationSerializer().toFile(conf, configFile);

      //Serialize the Configuration into a text file for easy read
      new AvroConfigurationSerializer().toTextFile(conf, configTextFile);
    } catch (final IOException e) {
      throw new RuntimeException("Cannot create driver configuration file at " + configFile.getAbsolutePath());
    }
  }

  /**
   * Return folder reef-bridge-project\reef-bridge-java\target\classes
   * @param fileName
   * @return
   */
  private static String getConfigFileFolder(final String fileName) {
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