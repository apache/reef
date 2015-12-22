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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

/**
 * Report of a tasklet execution result.
 */
@Unstable
@Private
@DriverSide
public final class TaskletResultReport implements TaskletReport {
  private final int taskletId;
  private final byte[] serializedResult;

  /**
   * @param taskletId of the Tasklet.
   * @param serializedResult of the tasklet execution in a serialized form.
   */
  public TaskletResultReport(final int taskletId, final byte[] serializedResult) {
    this.taskletId = taskletId;
    this.serializedResult = serializedResult;
  }

  /**
   * @return the type of this TaskletReport.
   */
  @Override
  public TaskletReportType getType() {
    return TaskletReportType.TaskletResult;
  }

  /**
   * @return the TaskletId of this TaskletReport
   */
  public int getTaskletId() {
    return taskletId;
  }

  /**
   * @return the result of the tasklet execution in a serialized form.
   */
  public byte[] getSerializedResult() {
    return serializedResult;
  }

}
