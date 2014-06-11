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
