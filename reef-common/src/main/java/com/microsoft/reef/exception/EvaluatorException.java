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
package com.microsoft.reef.exception;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.naming.Identifiable;

import java.util.concurrent.ExecutionException;

/**
 * Exception thrown to the Driver when an Evaluator becomes unusable.
 *
 */
@DriverSide
public class EvaluatorException extends ExecutionException implements Identifiable {

  private static final long serialVersionUID = 1L;
  private final transient String evaluatorId;
  private final transient RunningTask runningTask;

  public EvaluatorException(final String evaluatorId) {
    super();
    this.evaluatorId = evaluatorId;
    this.runningTask = null;
  }

  public EvaluatorException(final String evaluatorId, final String message, final Throwable cause) {
    super(message, cause);
    this.evaluatorId = evaluatorId;
    this.runningTask = null;
  }

  public EvaluatorException(final String evaluatorId, final String message) {
    this(evaluatorId, message, (RunningTask) null);
  }

  public EvaluatorException(final String evaluatorId, final String message, final RunningTask runningTask) {
    super(message);
    this.evaluatorId = evaluatorId;
    this.runningTask = runningTask;
  }

  public EvaluatorException(final String evaluatorId, final Throwable cause) {
    this(evaluatorId, cause, null);
  }

  public EvaluatorException(final String evaluatorId, final Throwable cause, final RunningTask runningTask) {
    super(cause);
    this.evaluatorId = evaluatorId;
    this.runningTask = runningTask;
  }

  /**
   * Access the affected Evaluator.
   *
   * @return the affected Evaluator.
   */
  @Override
  public String getId() {
    return this.evaluatorId;
  }

  public final RunningTask getRunningTask() {
    return this.runningTask;
  }
}
