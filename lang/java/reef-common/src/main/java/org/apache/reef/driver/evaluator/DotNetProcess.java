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

package org.apache.reef.driver.evaluator;

import org.apache.reef.runtime.common.launch.DotNetLaunchCommandBuilder;

import java.util.Collections;
import java.util.List;

/**
 * DotNet evaluator process.
 */
public final class DotNetProcess implements EvaluatorProcess {
  private final DotNetLaunchCommandBuilder commandBuilder = new DotNetLaunchCommandBuilder();
  private boolean optionSet = false;

  /**
   * Instantiated via DotNetProcessFactory.
   */
  DotNetProcess() {
  }

  @Override
  public List<String> getCommandLine() {
    return commandBuilder
        .build();
  }

  @Override
  public EvaluatorType getType() {
    return EvaluatorType.DOTNET;
  }

  @Override
  public DotNetProcess setMemory(final int megaBytes) {
    commandBuilder.setMemory(megaBytes);
    optionSet = true;
    return this;
  }

  @Override
  public boolean isOptionSet() {
    return optionSet;
  }

  @Override
  public DotNetProcess setConfigurationFileName(final String configurationFileName) {
    commandBuilder.setConfigurationFilePaths(Collections.singletonList(configurationFileName));
    return this;
  }

  @Override
  public DotNetProcess setStandardOut(final String standardOut) {
    commandBuilder.setStandardOut(standardOut);
    return this;
  }

  @Override
  public DotNetProcess setStandardErr(final String standardErr) {
    commandBuilder.setStandardErr(standardErr);
    return this;
  }
}
