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
package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 *  Vortex Worker configuration.
 */
@Unstable
@DriverSide
public final class VortexWorkerConf extends ConfigurationModuleBuilder {
  /**
   * Worker Threads.
   */
  @NamedParameter(doc = "Number of Worker Threads")
  public final class NumOfThreads implements Name<Integer> {
  }

  /**
   * Worker Threads.
   */
  public static final RequiredParameter<Integer> NUM_OF_THREADS = new RequiredParameter<>();

  /**
   * Vortex Worker configuration.
   */
  public static final ConfigurationModule CONF = new VortexWorkerConf()
      .bindNamedParameter(NumOfThreads.class, NUM_OF_THREADS)
      .build();
}
