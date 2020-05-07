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
package org.apache.reef.driver.task;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.runtime.common.driver.task.TaskRepresenter;

/**
 * Represents a running Task.
 */
@DriverSide
@Public
@Provided
public interface RunningTask extends Identifiable, AutoCloseable {


  /**
   * @return the context the task is running on.
   */
  ActiveContext getActiveContext();


  /**
   * Sends the message to the running task.
   *
   * @param message to be sent to the running task
   */
  void send(byte[] message);

  /**
   * Signal the task to suspend.
   *
   * @param message a message that is sent to the Task.
   */
  void suspend(byte[] message);

  /**
   * Signal the task to suspend.
   */
  void suspend();

  /**
   * Signal the task to shut down.
   *
   * @param message a message that is sent to the Task.
   */
  void close(byte[] message);

  /**
   * Signal the task to shut down.
   */
  @Override
  void close();

  /**
   * Gets the representer of task.
   * @return the representer of task
   */
  TaskRepresenter getTaskRepresenter();
}
