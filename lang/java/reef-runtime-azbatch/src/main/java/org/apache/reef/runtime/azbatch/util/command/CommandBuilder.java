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
package org.apache.reef.runtime.azbatch.util.command;

import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;

/**
 * Build the launch command for Java REEF processes for Azure Batch.
 */
public interface CommandBuilder {

  /**
   * Assembles the command to execute the Driver.
   *
   * @param jobSubmissionEvent the submission event for which to build the command.
   * @return the command string.
   */
  String buildDriverCommand(JobSubmissionEvent jobSubmissionEvent);

  /**
   * Assembles the command to execute the Evaluator Shim.
   *
   * @param evaluatorShimMemory the maximum size in, in bytes, of the memory allocation pool.
   * @param configurationPath   the relative path to the configuration file.
   * @return the command string.
   */
  String buildEvaluatorShimCommand(int evaluatorShimMemory, String configurationPath);

  /**
   * Assembles the command to execute the Evaluator.
   *
   * @param resourceLaunchEvent the launch event for which to build the command.
   * @param containerMemory the maximum size in, in bytes, of the memory allocation pool.
   * @param jvmHeapFactor a multiplicative factor to increase the container memory.
   * @return the command string.
   */
  String buildEvaluatorCommand(ResourceLaunchEvent resourceLaunchEvent,
                               int containerMemory, double jvmHeapFactor);

  /**
   * Returns the path to a file where the ip address is persisted.
   *
   * @return path to the file.
   */
  String getIpAddressFilePath();

  /**
   * Returns a command line that saves the ip address of the host to a file.
   *
   * @return command line string.
   */
  String captureIpAddressCommandLine();
}
