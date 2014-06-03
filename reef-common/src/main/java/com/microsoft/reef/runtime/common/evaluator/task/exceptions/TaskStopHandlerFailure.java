package com.microsoft.reef.runtime.common.evaluator.task.exceptions;

import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.task.events.TaskStop;
import com.microsoft.wake.EventHandler;

/**
 * Thrown when a TaskStop handler throws an exception
 */
@EvaluatorSide
@Private
public final class TaskStopHandlerFailure extends Exception {

  /**
   * @param cause the exception thrown by the stop handler
   */
  public TaskStopHandlerFailure(final EventHandler<TaskStop> handler, final Throwable cause) {
    super("EventHandler<TaskStop> `" + handler.toString() + "` threw an Exception in onNext()", cause);
  }
}
