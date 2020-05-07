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

import java.util.List;

/**
 * Defines the setup of an evaluator's process.
 * Users can set custom options via {@link #setMemory(int)} and process-type specific methods.
 * Runtimes can also set options, but should not do so if users have set options by
 * checking {@link #isOptionSet()}.
 */
public interface EvaluatorProcess {
  /**
   * @return the launch command line
   */
  List<String> getCommandLine();

  /**
   * @return the type of the evaluator
   */
  EvaluatorType getType();

  /**
   * Set memory size of process in megabytes.
   *
   * @param megaBytes
   * @return this
   */
  EvaluatorProcess setMemory(int megaBytes);

  /**
   * Check whether custom options have been set for the process.
   * Options are memory given via {@link #setMemory(int)} and any other custom options
   * supported by the specific type of evaluator process.
   *
   * @return whether custom options have been set for the process
   */
  boolean isOptionSet();

  /**
   * Set the name of the configuration file for the Launcher. This file is assumed to exist in the working directory of
   * the process launched with this command line.
   *
   * @param configurationFileName
   * @return this
   */
  EvaluatorProcess setConfigurationFileName(String configurationFileName);

  /**
   * Names a file to which stdout will be redirected.
   *
   * @param standardOut
   * @return this
   */
  EvaluatorProcess setStandardOut(String standardOut);

  /**
   * Names a file to which stderr will be redirected.
   *
   * @param standardErr
   * @return this
   */
  EvaluatorProcess setStandardErr(String standardErr);
}
