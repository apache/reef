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
 * Defines the setup of an evaluator's process
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
   * Set the error handler remote identifier
   *
   * @param errorHandlerRID
   * @return this
   */
  EvaluatorProcess setErrorHandlerRID(final String errorHandlerRID);

  /**
   * Set the launch identifier
   *
   * @param launchID
   * @return this
   */
  EvaluatorProcess setLaunchID(final String launchID);

  /**
   * Set memory size of process in megabytes
   *
   * @param megaBytes
   * @return this
   */
  EvaluatorProcess setMemory(final int megaBytes);

  /**
   * Set the name of the configuration file for the Launcher. This file is assumed to exist in the working directory of
   * the process launched with this command line.
   *
   * @param configurationFileName
   * @return this
   */
  EvaluatorProcess setConfigurationFileName(final String configurationFileName);

  /**
   * Names a file to which stdout will be redirected.
   *
   * @param standardOut
   * @return this
   */
  EvaluatorProcess setStandardOut(final String standardOut);

  /**
   * Names a file to which stderr will be redirected.
   *
   * @param standardErr
   * @return this
   */
  EvaluatorProcess setStandardErr(final String standardErr);
}
