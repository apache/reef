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
package org.apache.reef.runtime.common.launch;

import org.apache.reef.annotations.audience.Private;

import java.util.List;

/**
 * The formatter that creates and formats the Java command-line command.
 * The configuration file name is always the last parameter and the Launcher class name is always the first.
 * Flags are all the parameters between the Launcher class and the configuration file name.
 * Currently there are 2 types of Launchers: BootstrapLauncher and REEFLauncher.
 * The BootstrapLauncher is mainly used to launch the .NET Driver without Java dependencies and has a flag
 * to internally create the Java Driver configuration and invoke REEFLauncher with the newly created
 * configuration file.
 */
@Private
public interface LauncherCommandFormatter {
  /**
   * @return the Launcher class name.
   */
  String getLauncherClass();

  /**
   * @param flag Adds a flag to the command line.
   * @return LauncherCommandFormatter
   */
  LauncherCommandFormatter addFlag(final String flag);

  /**
   * @return List of flags for the command line.
   */
  List<String> getFlags();

  /**
   * @param configurationFileName The name of the configuration file.
   * @return LauncherCommandFormatter
   */
  LauncherCommandFormatter setConfigurationFileName(final String configurationFileName);

  /**
   * @return The configuration file name.
   */
  String getConfigurationFileName();
}
