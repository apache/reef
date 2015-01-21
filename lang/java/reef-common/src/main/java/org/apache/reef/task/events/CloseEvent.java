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
package org.apache.reef.task.events;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.util.Optional;

/**
 * Indicates that the driver called .close() on this Task.
 */
@TaskSide
@Provided
@Public
public interface CloseEvent {

  /**
   * @return the message sent with the close call, if any.
   */
  public Optional<byte[]> get();
}
