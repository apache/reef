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
package org.apache.reef.runtime.azbatch.util;

import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.TaskExecutionResult;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;

/**
 * Class that maps status of Azure Batch task to a REEF task.
 */
public final class TaskStatusMapper {

  private TaskStatusMapper() {
  }

  /**
   * Get the {@link State} from a {@link CloudTask}.
   *
   * @param task the task.
   * @return the state of the task.
   */
  public static State getReefTaskState(final CloudTask task) {
    switch (task.state()) {
    case ACTIVE:
      return State.INIT;
    case RUNNING:
      return State.RUNNING;
    case COMPLETED:
      if (task.executionInfo().result() == TaskExecutionResult.SUCCESS) {
        return State.DONE;
      } else {
        return State.FAILED;
      }
    case PREPARING:
      return State.RUNNING;
    default:
      throw new IllegalArgumentException("Azure batch cloud task has unknown state: " + task.state());
    }
  }
}
