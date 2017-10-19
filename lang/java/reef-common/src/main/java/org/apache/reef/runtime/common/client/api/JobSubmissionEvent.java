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
package org.apache.reef.runtime.common.client.api;

import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

import java.util.Set;

/**
 * Event sent to Driver Runtime.
 * Submission of a Job to the Driver Runtime
 */
@RuntimeAuthor
@DefaultImplementation(JobSubmissionEventImpl.class)
public interface JobSubmissionEvent {

  /**
   * @return Id of the Job
   */
  String getIdentifier();

  /**
   * @return Remote Id for the error handler
   */
  String getRemoteId();

  /**
   * @return Driver configuration
   */
  Configuration getConfiguration();

  /**
   * @return Owner's username
   */
  String getUserName();

  /**
   * @return List of global files
   */
  Set<FileResource> getGlobalFileSet();

  /**
   * @return List of local files
   */
  Set<FileResource> getLocalFileSet();

  /**
   * @return Memory to be allocated to the Driver
   */
  Optional<Integer> getDriverMemory();

  /**
   * @return The number of CPU cores to be allocated for the Driver
   */
  Optional<Integer> getDriverCPUCores();

  /**
   * @return Priority to be given to the Job
   */
  Optional<Integer> getPriority();

  /**
   * @return True if evaluators are to be preserved across driver failures.
   */
  Optional<Boolean> getPreserveEvaluators();

  /**
   * Returns the number of time that the driver should be started by the resource manager
   * if it fails unexpectedly.
   */
  Optional<Integer> getMaxApplicationSubmissions();
}
