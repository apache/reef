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
package org.apache.reef.io.data.output;

/**
 * A provider through which users create task output streams.
 */
public abstract class TaskOutputStreamProvider implements OutputStreamProvider {

  /**
   * id of the current task.
   */
  private String taskId;

  /**
   * set the id of the current task.
   *
   * @param taskId id of the current task
   */
  protected final void setTaskId(final String taskId) {
    this.taskId = taskId;
  }

  /**
   * get the id of the current task.
   *
   * @return id of the current task
   */
  protected final String getTaskId() {
    return this.taskId;
  }
}
