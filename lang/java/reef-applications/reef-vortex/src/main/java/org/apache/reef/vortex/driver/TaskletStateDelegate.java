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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import java.io.Serializable;

/**
 * A Delegate used by the VortexMaster to trigger and query state transitions of a Tasklet.
 */
@Private
@DriverSide
public interface TaskletStateDelegate<TOutput extends Serializable> {

  /**
   * Used by the VortexMaster to query whether the Tasklet is done.
   * @param taskletId the ID of the Tasklet to query.
   * @return true if the Tasklet completed, failed, or was cancelled; false otherwise.
   */
  boolean isDone(final int taskletId);

  /**
   * Called by VortexMaster to let the user know that the task completed.
   * @param taskletId the ID of the Tasklet that completed.
   * @param result the result of the Tasklet computation
   */
  void completed(final int taskletId, final TOutput result);

  /**
   * Called by VortexMaster to let the user know that the Tasklet was cancelled.
   * @param taskletId the ID of the Tasklet that was cancelled.
   */
  void cancelled(final int taskletId);

  /**
   * Called by VortexMaster to let the user know that the task threw an exception.
   * @param taskletId the ID of the Tasklet that threw an Exception.
   * @param exception the Exception that was thrown in the Tasklet.
   */
  void threwException(final int taskletId, final Exception exception);
}
