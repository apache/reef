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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Report of a tasklet exception on aggregation.
 */
@Unstable
public final class TaskletAggregationFailureReport implements WorkerToMasterReport {
  private List<Integer> taskletIds;
  private Exception exception;

  /**
   * No-arg constructor required for Kryo to serialize/deserialize.
   */
  TaskletAggregationFailureReport() {
  }

  /**
   * @param taskletIds of the failed tasklet(s).
   * @param exception that caused the tasklet failure.
   */
  public TaskletAggregationFailureReport(final List<Integer> taskletIds, final Exception exception) {
    this.taskletIds = Collections.unmodifiableList(new ArrayList<>(taskletIds));
    this.exception = exception;
  }

  /**
   * @return the type of this TaskletReport.
   */
  @Override
  public Type getType() {
    return Type.TaskletAggregationFailure;
  }

  /**
   * @return the taskletIds that failed on aggregation.
   */
  public List<Integer> getTaskletIds() {
    return taskletIds;
  }

  /**
   * @return the exception that caused the tasklet aggregation failure.
   */
  public Exception getException() {
    return exception;
  }
}
