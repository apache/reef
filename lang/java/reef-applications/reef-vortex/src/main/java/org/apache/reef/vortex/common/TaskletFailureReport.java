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
package org.apache.reef.vortex.common;

import org.apache.reef.annotations.Unstable;

/**
 * Report of a tasklet exception.
 */
@Unstable
public final class TaskletFailureReport implements TaskletReport {
  private final int taskletId;
  private final Exception exception;

  /**
   * @param taskletId of the failed tasklet.
   * @param exception that caused the tasklet failure.
   */
  public TaskletFailureReport(final int taskletId, final Exception exception) {
    this.taskletId = taskletId;
    this.exception = exception;
  }

  /**
   * @return the type of this TaskletReport.
   */
  @Override
  public TaskletReportType getType() {
    return TaskletReportType.TaskletFailure;
  }

  /**
   * @return the id of the tasklet.
   */
  @Override
  public int getTaskletId() {
    return taskletId;
  }

  /**
   * @return the exception that caused the tasklet failure.
   */
  public Exception getException() {
    return exception;
  }
}