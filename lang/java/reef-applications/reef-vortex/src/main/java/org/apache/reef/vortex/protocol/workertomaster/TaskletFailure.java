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
package org.apache.reef.vortex.protocol.workertomaster;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

/**
 * Report of a Tasklet exception.
 */
@Unstable
@Private
@DriverSide
public final class TaskletFailure implements WorkerToMaster {
  private int taskletId;
  private Exception exception;

  /**
   * No-arg constructor required for Kryo to ser/des.
   */
  TaskletFailure() {
  }

  /**
   * @param taskletId of the failed Tasklet.
   * @param exception that caused the tasklet failure.
   */
  public TaskletFailure(final int taskletId, final Exception exception) {
    this.taskletId = taskletId;
    this.exception = exception;
  }

  /**
   * @return the type of this TaskletReport.
   */
  @Override
  public Type getType() {
    return Type.TaskletFailure;
  }

  /**
   * @return the taskletId of this TaskletReport.
   */
  public int getTaskletId() {
    return taskletId;
  }

  /**
   * @return the exception that caused the Tasklet failure.
   */
  public Exception getException() {
    return exception;
  }
}