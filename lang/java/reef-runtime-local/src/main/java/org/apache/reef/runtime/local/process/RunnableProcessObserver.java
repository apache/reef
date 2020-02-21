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
package org.apache.reef.runtime.local.process;

/**
 * Observer interface for events fired by RunnableProcess.
 */
public interface RunnableProcessObserver {
  /**
   * This will be called right after the process is launched.
   *
   * @param processId the id of the process that started.
   */
  void onProcessStarted(String processId);

  /**
   * This will be called right after the process exited.
   *
   * @param exitCode  the return code of the process.
   * @param processId the id of the process that exited.
   */
  void onProcessExit(String processId, int exitCode);
}
