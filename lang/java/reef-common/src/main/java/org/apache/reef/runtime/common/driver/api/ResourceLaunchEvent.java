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
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * Event from Driver Process to Driver Runtime.
 * A request to the Driver Runtime to launch an Evaluator on the allocated Resource
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(ResourceLaunchEventImpl.class)
public interface ResourceLaunchEvent {

    /**
     * @return Id of the resource to launch the Evaluator on
     */
  String getIdentifier();

    /**
     * @return Remote Id for the error handler
     */
  String getRemoteId();

    /**
     * @return Evaluator configuration
     */
  Configuration getEvaluatorConf();

    /**
     * @return Evaluator process to launch
     */
  EvaluatorProcess getProcess();

    /**
     * @return List of libraries local to this Evaluator
     */
  Set<FileResource> getFileSet();

  /**
   * @return name of the runtime to launch the Evaluator on
   */
  String getRuntimeName();
}
