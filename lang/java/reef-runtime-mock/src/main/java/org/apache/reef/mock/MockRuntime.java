/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock;

/**
 * Mock API used to drive the evaluation of ProcessRequest
 * events, which are triggered by the Application driver.
 * Clients used this to determine whether a particular ProcessRequest
 * event should succeed or fail.
 */
public interface MockRuntime extends MockFailure {

  /**
   * Initiate the start time event to the application driver.
   */
  void start();

  /**
   * Initiate the stop time event to the application driver.
   */
  void stop();

  /**
   * @return true if there is an outstanding ProcessRequest
   */
  boolean hasProcessRequest();

  /**
   * The client (caller) is responsible for determining what
   * to do with a ProcessRequest event. There are three options:
   * 1. Pass to the succeed method, which signals success to the driver.
   * 2. Pass to the fail method, signaling failure to the driver.
   * 3. Drop it on the floor (e.g., network failure).
   *
   * @return the next ProcessRequest object to be processed.
   */
  ProcessRequest getNextProcessRequest();

  /**
   * The driver will be informed that the operation corresponding
   * to the ProcessRequest succeeded, and will be given any relevant
   * data structures e.g., AllocatedEvaluator, RunningTask, etc.
   *
   * @param request to be processed successfully
   */
  void succeed(final ProcessRequest request);

  /**
   * The driver will be informed that the operation corresponding
   * to the PRocessRequest failed, and will be given any relevant
   * data structures e.g., FailedEvaluator, FailedTask, etc.
   *
   * @param request to be failed.
   */
  void fail(final ProcessRequest request);
}
