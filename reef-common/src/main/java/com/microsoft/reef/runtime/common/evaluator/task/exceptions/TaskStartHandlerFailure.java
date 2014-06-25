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
package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.task.events.TaskStart;
import com.microsoft.wake.EventHandler;

/**
 * Thrown when a TastStart handler throws an exception
 */
@EvaluatorSide
@Private
public final class TaskStartHandlerFailure extends Exception {

  /**
   * @param cause the exception thrown by the start handler
   */
  public TaskStartHandlerFailure(final EventHandler<TaskStart> handler, final Throwable cause) {
    super("EventHandler<TaskStart> `" + handler.toString() + "` threw an Exception in onNext()", cause);
  }
}
