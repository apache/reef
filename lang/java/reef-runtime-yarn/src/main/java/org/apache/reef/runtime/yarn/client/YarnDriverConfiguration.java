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
package org.apache.reef.runtime.yarn.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.evaluator.SchedulingConstraintBuilder;
import org.apache.reef.runtime.yarn.driver.evaluator.YarnSchedulingConstraintBuilder;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * Additional YARN-Specific configuration options to be merged with DriverConfiguration.
 */
@Public
@ClientSide
public final class YarnDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * The queue to submit this Driver to.
   */
  public static final OptionalParameter<String> QUEUE = new OptionalParameter<>();
  
  /**
   * The job submission directory.
   */
  public static final OptionalParameter<String> JOB_SUBMISSION_DIRECTORY_PREFIX = new OptionalParameter<>();

  /**
   * ConfigurationModule to set YARN-Specific configuration options to be merged with DriverConfiguration.
   */
  public static final ConfigurationModule CONF = new YarnDriverConfiguration()
      .bindNamedParameter(JobQueue.class, QUEUE)
      .bindNamedParameter(JobSubmissionDirectoryPrefix.class, JOB_SUBMISSION_DIRECTORY_PREFIX)
      .bindImplementation(SchedulingConstraintBuilder.class, YarnSchedulingConstraintBuilder.class)
      .build();
}
