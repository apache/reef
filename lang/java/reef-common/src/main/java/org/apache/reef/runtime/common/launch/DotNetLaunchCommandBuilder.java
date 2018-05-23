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

import org.apache.commons.lang.StringUtils;
import org.apache.reef.util.OSUtils;
import org.apache.reef.util.Optional;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for the command line to launch a DotNent Evaluator.
 */
public final class DotNetLaunchCommandBuilder implements LaunchCommandBuilder {
  private static final Logger LOG = Logger.getLogger(DotNetLaunchCommandBuilder.class.getName());
  private static final String EVALUATOR_DLL_PATH = "reef/global/Org.Apache.REEF.Evaluator.dll";
  private static final String EVALUATOR_EXE_PATH = "reef/global/Org.Apache.REEF.Evaluator.exe";

  private String standardErrPath = null;
  private String standardOutPath = null;
  private int megaBytes = 0;
  private Optional<List<String>> evaluatorConfigurationPaths = Optional.empty();

  @Override
  public List<String> build() {
    final List<String> result = new LinkedList<>();
    final File dll = new File(EVALUATOR_DLL_PATH);
    final File exe = new File(EVALUATOR_EXE_PATH);
    if (OSUtils.isWindows() && exe.exists()) {
      LOG.log(Level.WARNING, "Using executable file: {0}", exe.getPath());
      result.add(exe.getPath());
    } else if (dll.exists()) {
      LOG.log(Level.WARNING, "Using dll executable file: {0}", dll.getPath());
      result.add("dotnet");
      result.add(dll.getPath());
    } else {
      LOG.log(Level.WARNING, "executable file can NOT be found");
    }
    if (evaluatorConfigurationPaths.isPresent()) {
      for (final String evaluatorConfigurationPath : evaluatorConfigurationPaths.get()) {
        result.add(evaluatorConfigurationPath);
      }
    }
    if (null != this.standardOutPath && !standardOutPath.isEmpty()) {
      result.add(">" + this.standardOutPath);
    }
    if (null != this.standardErrPath && !standardErrPath.isEmpty()) {
      result.add("2>" + this.standardErrPath);
    }
    LOG.log(Level.INFO, "Launch Exe: {0}", StringUtils.join(result, ' '));
    return result;
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public DotNetLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public DotNetLaunchCommandBuilder setConfigurationFilePaths(final List<String> configurationFilePaths) {
    this.evaluatorConfigurationPaths = Optional.of(configurationFilePaths);
    return this;
  }

  @Override
  public DotNetLaunchCommandBuilder setStandardOut(final String standardOut) {
    this.standardOutPath = standardOut;
    return this;
  }

  @Override
  public DotNetLaunchCommandBuilder setStandardErr(final String standardErr) {
    this.standardErrPath = standardErr;
    return this;
  }
}
