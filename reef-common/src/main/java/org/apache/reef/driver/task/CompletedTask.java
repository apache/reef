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
package org.apache.reef.driver.task;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.io.Message;
import org.apache.reef.io.naming.Identifiable;

/**
 * Represents a completed Task.
 */
@DriverSide
@Public
@Provided
public interface CompletedTask extends Message, Identifiable {

  /**
   * @return the context the Task ran on.
   */
  public ActiveContext getActiveContext();

  /**
   * @return the id of the completed task.
   */
  @Override
  public String getId();
}
