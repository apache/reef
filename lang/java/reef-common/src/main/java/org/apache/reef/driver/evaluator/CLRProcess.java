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

import org.apache.reef.runtime.common.launch.CLRLaunchCommandBuilder;

import java.util.List;

/**
 * Defines the setup of a CLR process
 */
public final class CLRProcess implements EvaluatorProcess {
  private final CLRLaunchCommandBuilder commandBuilder = new CLRLaunchCommandBuilder();

  /**
   * Instantiated via CLRProcessFactory
   */
  CLRProcess() {
  }

  @Override
  public List<String> getCommandLine() {
    return commandBuilder
        .build();
  }

  @Override
  public EvaluatorType getType() {
    return EvaluatorType.CLR;
  }

  @Override
  public EvaluatorProcess setErrorHandlerRID(final String errorHandlerRID) {
    commandBuilder.setErrorHandlerRID(errorHandlerRID);
    return this;
  }

  @Override
  public EvaluatorProcess setLaunchID(final String launchID) {
    commandBuilder.setLaunchID(launchID);
    return this;
  }

  @Override
  public EvaluatorProcess setMemory(final int megaBytes) {
    commandBuilder.setMemory(megaBytes);
    return this;
  }

  @Override
  public EvaluatorProcess setConfigurationFileName(final String configurationFileName) {
    commandBuilder.setConfigurationFileName(configurationFileName);
    return this;
  }

  @Override
  public EvaluatorProcess setStandardOut(final String standardOut) {
    commandBuilder.setStandardOut(standardOut);
    return this;
  }

  @Override
  public EvaluatorProcess setStandardErr(final String standardErr) {
    commandBuilder.setStandardErr(standardErr);
    return this;
  }
}
