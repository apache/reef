package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;

/**
 * Thrown when a Task Message Handler throws an exception
 */
@EvaluatorSide
@Private
public final class TaskMessageHandlerFailure extends Exception {

  /**
   * the exception thrown by the task message handler's onNext() method.
   */
  public TaskMessageHandlerFailure(final Throwable cause) {
    super("Task message handler threw an Exception.", cause);
  }
}
