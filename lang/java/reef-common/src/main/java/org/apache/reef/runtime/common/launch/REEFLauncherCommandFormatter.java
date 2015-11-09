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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.REEFLauncher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Java command formatting class for the REEFLauncher class.
 * Directly launches the Driver, assuming a pre-generated Java Driver configuration.
 * @see LauncherCommandFormatter
 */
@Private
public final class REEFLauncherCommandFormatter implements LauncherCommandFormatter {
  private final List<String> immutableFlags = Collections.unmodifiableList(new ArrayList<String>());
  private String configFileName;

  private REEFLauncherCommandFormatter(){
  }

  public static REEFLauncherCommandFormatter getLauncherCommand() {
    return new REEFLauncherCommandFormatter();
  }

  @Override
  public LauncherCommandFormatter addFlag(final String flag) {
    throw new NotImplementedException("Launching with the REEFLauncher does not support flags.");
  }

  @Override
  public String getLauncherClass() {
    return REEFLauncher.class.getName();
  }

  @Override
  public List<String> getFlags() {
    return immutableFlags;
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
