/*
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
package org.apache.reef.runtime.common.evaluator.task;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.runtime.common.evaluator.task.exceptions.TaskStartHandlerFailure;
import org.apache.reef.runtime.common.evaluator.task.exceptions.TaskStopHandlerFailure;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Convenience class to send task start and stop events.
 */
@EvaluatorSide
@Private
final class TaskLifeCycleHandlers {
  private static final Logger LOG = Logger.getLogger(TaskLifeCycleHandlers.class.getName());
  private final Set<EventHandler<TaskStop>> taskStopHandlers;
  private final Set<EventHandler<TaskStart>> taskStartHandlers;
  private final TaskStart taskStart;
  private final TaskStop taskStop;

  @Inject
  TaskLifeCycleHandlers(@Parameter(TaskConfigurationOptions.StopHandlers.class)
                        final Set<EventHandler<TaskStop>> taskStopHandlers,
                        @Parameter(TaskConfigurationOptions.StartHandlers.class)
                        final Set<EventHandler<TaskStart>> taskStartHandlers,
                        final TaskStartImpl taskStart,
                        final TaskStopImpl taskStop) {
    this.taskStopHandlers = taskStopHandlers;
    this.taskStartHandlers = taskStartHandlers;
    this.taskStart = taskStart;
    this.taskStop = taskStop;
  }

  /**
   * Sends the TaskStart event to the handlers for it.
   */
  @SuppressWarnings("checkstyle:illegalcatch")
  public void beforeTaskStart() throws TaskStartHandlerFailure {
    LOG.log(Level.FINEST, "Sending TaskStart event to the registered event handlers.");
    for (final EventHandler<TaskStart> startHandler : this.taskStartHandlers) {
      try {
        startHandler.onNext(this.taskStart);
      } catch (final Throwable throwable) {
        throw new TaskStartHandlerFailure(startHandler, throwable);
      }
    }
    LOG.log(Level.FINEST, "Done sending TaskStart event to the registered event handlers.");
  }

  /**
   * Sends the TaskStop event to the handlers for it.
   */
  @SuppressWarnings("checkstyle:illegalcatch")
  public void afterTaskExit() throws TaskStopHandlerFailure {
    LOG.log(Level.FINEST, "Sending TaskStop event to the registered event handlers.");
    for (final EventHandler<TaskStop> stopHandler : this.taskStopHandlers) {
      try {
        stopHandler.onNext(this.taskStop);
      } catch (final Throwable throwable) {
        throw new TaskStopHandlerFailure(stopHandler, throwable);
      }
    }
    LOG.log(Level.FINEST, "Done sending TaskStop event to the registered event handlers.");
  }


}
