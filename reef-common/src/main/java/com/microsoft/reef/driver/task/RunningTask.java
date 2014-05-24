/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.driver.task;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.io.naming.Identifiable;

/**
 * Represents a running Task
 */
@DriverSide
@Public
@Provided
public interface RunningTask extends Identifiable, AutoCloseable {


  /**
   * @return the context the task is running on.
   */
  public ActiveContext getActiveContext();


  /**
   * Sends the message to the running task.
   *
   * @param message to be sent to the running task
   */
  public void send(final byte[] message);

  /**
   * Signal the task to suspend.
   *
   * @param message a message that is sent to the Task.
   */
  public void suspend(final byte[] message);

  /**
   * Signal the task to suspend.
   */
  public void suspend();

  /**
   * Signal the task to shut down.
   *
   * @param message a message that is sent to the Task.
   */
  public void close(final byte[] message);

  /**
   * Signal the task to shut down.
   */
  @Override
  public void close();
}
