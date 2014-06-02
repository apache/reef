package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;

/**
 * Thrown when a Task.call() throws an exception
 */
@EvaluatorSide
@Private
public final class TaskCallFailure extends Exception {

  /**
   * @param cause the exception thrown by the Task.call() method.
   */
  public TaskCallFailure(final Throwable cause) {
    super("Task.call() threw an Exception.", cause);
  }
}
