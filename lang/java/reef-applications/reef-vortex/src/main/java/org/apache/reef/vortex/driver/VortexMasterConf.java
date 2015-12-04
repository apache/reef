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
import org.apache.reef.tang.formats.*;
import org.apache.reef.vortex.api.VortexStart;

/**
 * Vortex Master configuration.
 */
@Unstable
@DriverSide
public final class VortexMasterConf extends ConfigurationModuleBuilder {
  /**
   * Number of Workers.
   */
  @NamedParameter(doc = "Number of Workers")
  final class WorkerNum implements Name<Integer> {
  }

  /**
   * Worker Memory.
   */
  @NamedParameter(doc = "Worker Memory")
  final class WorkerMem implements Name<Integer> {
  }

  /**
   * Worker Cores.
   */
  @NamedParameter(doc = "Worker Cores")
  final class WorkerCores implements Name<Integer> {
  }

  /**
   * Worker Capacity.
   */
  @NamedParameter(doc = "Worker Capacity")
  final class WorkerCapacity implements Name<Integer> {
  }

  /**
   * Number of Vortex Start Threads.
   */
  @NamedParameter(doc = "Number of Vortex Start Threads")
  final class NumberOfVortexStartThreads implements Name<Integer> {
  }

  /**
   * Size of threadpool for callbacks on {@link org.apache.reef.vortex.api.VortexFuture}.
   */
  @NamedParameter(doc = "Size of threadpool for callbacks on VortexFuture.", default_value = "10")
  final class CallbackThreadpoolSize implements Name<Integer> {
  }

  /**
   * Number of Workers.
   */
  public static final RequiredParameter<Integer> WORKER_NUM = new RequiredParameter<>();

  /**
   * Worker Memory.
   */
  public static final RequiredParameter<Integer> WORKER_MEM = new RequiredParameter<>();

  /**
   * Worker Cores.
   */
  public static final RequiredParameter<Integer> WORKER_CORES = new RequiredParameter<>();

  /**
   * Worker Cores.
   */
  public static final OptionalParameter<Integer> WORKER_CAPACITY = new OptionalParameter<>();

  /**
   * Vortex Start.
   */
  public static final RequiredImpl<VortexStart> VORTEX_START = new RequiredImpl<>();

  /**
   * Number of Vortex Start threads.
   */
  public static final RequiredParameter<Integer> NUM_OF_VORTEX_START_THREAD = new RequiredParameter<>();

  /**
   * Size of threadpool for callbacks on VortexFuture.
   */
  public static final OptionalParameter<Integer> FUTURE_CALLBACK_THREADPOOL_SIZE = new OptionalParameter<>();

  /**
   * Vortex Master configuration.
   */
  public static final ConfigurationModule CONF = new VortexMasterConf()
      .bindNamedParameter(WorkerNum.class, WORKER_NUM)
      .bindNamedParameter(WorkerMem.class, WORKER_MEM)
      .bindNamedParameter(WorkerCores.class, WORKER_CORES)
      .bindNamedParameter(WorkerCapacity.class, WORKER_CAPACITY)
      .bindImplementation(VortexStart.class, VORTEX_START)
      .bindNamedParameter(NumberOfVortexStartThreads.class, NUM_OF_VORTEX_START_THREAD)
      .bindNamedParameter(CallbackThreadpoolSize.class, FUTURE_CALLBACK_THREADPOOL_SIZE)
      .build();
}
