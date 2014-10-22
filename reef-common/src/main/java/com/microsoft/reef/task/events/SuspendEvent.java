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
package com.microsoft.reef.task.events;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.util.Optional;

/**
 * Event fired when the driver called suspend() on this task.
 */
@TaskSide
@Provided
@Public
public interface SuspendEvent {

  /**
   * @return the message sent with the suspend call, if any.
   */
  public Optional<byte[]> get();
}
