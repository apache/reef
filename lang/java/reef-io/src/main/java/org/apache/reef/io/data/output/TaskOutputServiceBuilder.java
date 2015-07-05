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
package org.apache.reef.io.data.output;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Builder to create a TaskOutputService object.
 */
public final class TaskOutputServiceBuilder extends ConfigurationModuleBuilder {

  /**
   * A provider through which users create task output streams.
   */
  public static final RequiredImpl<TaskOutputStreamProvider> TASK_OUTPUT_STREAM_PROVIDER = new RequiredImpl<>();

  /**
   * Path of the directory where output files are created.
   */
  public static final RequiredParameter<String> OUTPUT_PATH = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new TaskOutputServiceBuilder()
      .bindImplementation(OutputService.class, TaskOutputService.class)
      .bindImplementation(TaskOutputStreamProvider.class, TASK_OUTPUT_STREAM_PROVIDER)
      .bindNamedParameter(TaskOutputService.OutputPath.class, OUTPUT_PATH)
      .build();
}
