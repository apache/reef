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
import com.microsoft.reef.common.AbstractFailure;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.util.Optional;

/**
 * An error message that REEF Driver gets from a failed Task.
 */
@DriverSide
@Provided
@Public
public final class FailedTask extends AbstractFailure {

  /**
   * (Optional) Context of the failed Task.
   */
  private final Optional<ActiveContext> context;

  /**
   * @param id          Identifier of the entity that produced the error. Cannot be null.
   * @param message     One-line error message. Cannot be null.
   * @param description Long error description. Can be null.
   * @param cause       Java Exception that caused the error. Can be null.
   * @param data        byte array that contains serialized version of the error. Can be null.
   * @param context     the Context the Task failed on.
   */
  public FailedTask(final String id,
                    final String message,
                    final Optional<String> description,
                    final Optional<Throwable> cause,
                    final Optional<byte[]> data,
                    final Optional<ActiveContext> context) {
    super(id, message, description, cause, data);
    this.context = context;
  }


  /**
   * Access the context the task ran (and crashed) on, if it could be recovered.
   * <p/>
   * An ActiveContext is given when the task fails but the context remains alive.
   * On context failure, the context also fails and is surfaced via the FailedContext event.
   * <p/>
   * Note that receiving an ActiveContext here is no guarantee that the context (and evaluator)
   * are in a consistent state. Application developers need to investigate the reason available
   * via getCause() to make that call.
   *
   * @return the context the Task ran on.
   */
  public Optional<ActiveContext> getActiveContext() {
    return this.context;
  }
}
