package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;

/**
 * Thrown when a Task Suspend Handler throws an exception
 */
@EvaluatorSide
@Private
public final class TaskSuspendHandlerFailure extends Exception {

  /**
   * @param cause the exception thrown by the task suspend handler's onNext() method.
   */
  public TaskSuspendHandlerFailure(final Throwable cause) {
    super("Task suspend handler threw an Exception.", cause);
  }
}
