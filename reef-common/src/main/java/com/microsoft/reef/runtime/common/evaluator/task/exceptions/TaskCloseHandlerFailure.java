package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;

/**
 * Thrown when a Task Close Handler throws an exception
 */
@EvaluatorSide
@Private
public final class TaskCloseHandlerFailure extends Exception {

  /**
   * @param cause the exception thrown by the Task.call() method.
   */
  public TaskCloseHandlerFailure(final Throwable cause) {
    super("Task close handler threw an Exception.", cause);
  }
}
