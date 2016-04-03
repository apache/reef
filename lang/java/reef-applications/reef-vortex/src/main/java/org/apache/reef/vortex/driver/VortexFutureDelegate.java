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
import org.apache.reef.annotations.audience.Private;

import java.util.List;

/**
 * Exposes functions to be called by the {@link org.apache.reef.vortex.driver.VortexMaster}
 * to note that a list of Tasklets associated with a Future has completed.
 */
@Unstable
@DriverSide
@Private
public interface VortexFutureDelegate<TOutput> {

  /**
   * A Tasklet associated with the future has completed with a result.
   */
  void completed(final int taskletId, TOutput result);

  /**
   * The list of aggregated Tasklets associated with the Future that have completed with a result.
   */
  void aggregationCompleted(final List<Integer> taskletIds, final TOutput result);

  /**
   * A Tasklet associated with the Future has thrown an Exception.
   */
  void threwException(final int taskletId, final Exception exception);

  /**
   * The list of Tasklets associated with the Future that have thrown an Exception.
   */
  void aggregationThrewException(final List<Integer> taskletIds, final Exception exception);

  /**
   * A Tasklet associated with the Future has been cancelled.
   */
  void cancelled(final int taskletId);
}
