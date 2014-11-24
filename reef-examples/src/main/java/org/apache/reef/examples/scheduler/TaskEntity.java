/**
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
package org.apache.reef.examples.scheduler;

/**
 * TaskEntity represent a single entry of task queue used in
 * scheduler. Since REEF already has the class named {Task},
 * a different name is used for this class.
 */
final class TaskEntity {
  private final int taskId;
  private final String command;

  public TaskEntity(final int taskId, final String command) {
    this.taskId = taskId;
    this.command = command;
  }

  /**
   * Return the TaskID assigned to this Task.
   */
  int getId() {
    return taskId;
  }

  String getCommand() {
    return command;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskEntity that = (TaskEntity) o;

    if (taskId != that.taskId) return false;
    if (!command.equals(that.command)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = taskId;
    result = 31 * result + command.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("<Id=").append(taskId).
      append(", Command=").append(command).append(">").toString();
  }
}
