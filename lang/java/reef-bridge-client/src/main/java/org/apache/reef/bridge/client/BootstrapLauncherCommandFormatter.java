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
package org.apache.reef.bridge.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.launch.LauncherCommandFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Creates the Java command line for the bootstrap Launcher class.
 * @see org.apache.reef.runtime.common.launch.LauncherCommandFormatter
 */
@Private
public final class BootstrapLauncherCommandFormatter implements LauncherCommandFormatter {
  private final List<String> flags = new ArrayList<>();
  private String configFileName = "";

  private BootstrapLauncherCommandFormatter() {
  }

  public static BootstrapLauncherCommandFormatter getLauncherCommand(){
    return new BootstrapLauncherCommandFormatter();
  }

  @Override
  public LauncherCommandFormatter addFlag(final String flag) {
    flags.add(flag);
    return this;
  }

  @Override
  public String getLauncherClass() {
    return BootstrapLauncher.class.getName();
  }

  @Override
  public List<String> getFlags() {
    return Collections.unmodifiableList(flags);
  }

  @Override
  public LauncherCommandFormatter setConfigurationFileName(final String configurationFileName) {
    configFileName = configurationFileName;
    return this;
  }

  @Override
  public String getConfigurationFileName() {
    return configFileName;
  }
}
