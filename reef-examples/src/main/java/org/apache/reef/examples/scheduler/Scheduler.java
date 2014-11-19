/**
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
package org.apache.reef.examples.scheduler;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The body of Task scheduler. It owns a task queue
 * and tracks the record of scheduled tasks.
 */
@ThreadSafe
final class Scheduler {
  /**
   * Tasks are waiting to be scheduled in the queue.
   */
  private final Queue<TaskEntity> taskQueue;

  /**
   * Lists of {@link TaskEntity} for different states - Running / Finished / Canceled.
   */
  private final List<TaskEntity> runningTasks = new ArrayList<>();
  private final List<TaskEntity> finishedTasks = new ArrayList<>();
  private final List<TaskEntity> canceledTasks = new ArrayList<>();

  /**
   * Counts how many tasks have been scheduled.
   */
  private final AtomicInteger taskCount = new AtomicInteger(0);

  @Inject
  public Scheduler() {
    taskQueue = new LinkedBlockingQueue<>();
  }

  /**
   * Submit a task to the ActiveContext.
   */
  public synchronized void submitTask(final ActiveContext context) {
    final TaskEntity task = taskQueue.poll();
    final Integer taskId = task.getId();
    final String command = task.getCommand();

    final Configuration taskConf = TaskConfiguration.CONF
      .set(TaskConfiguration.TASK, ShellTask.class)
      .set(TaskConfiguration.IDENTIFIER, taskId.toString())
      .build();
    final Configuration commandConf = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(Command.class, command)
      .build();

    final Configuration merged = Configurations.merge(taskConf, commandConf);
    context.submitTask(merged);
    runningTasks.add(task);
  }

  /**
   * Update the record of task to mark it as canceled.
   */
  public synchronized SchedulerResponse cancelTask(final int taskId) {
    if (getTask(taskId, runningTasks) != null) {
      return SchedulerResponse.FORBIDDEN("The task " + taskId + " is running");
    } else if (getTask(taskId, finishedTasks) != null) {
      return SchedulerResponse.FORBIDDEN("The task " + taskId + " has been finished");
    }

    final TaskEntity task = getTask(taskId, taskQueue);
    if (task == null) {
      final String message = new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString();
      return SchedulerResponse.NOT_FOUND(message);
    } else {
      taskQueue.remove(task);
      canceledTasks.add(task);
      return SchedulerResponse.OK("Canceled " + taskId);
    }
  }

  /**
   * Clear the pending list
   */
  public synchronized SchedulerResponse clear() {
    final int count = taskQueue.size();
    for (final TaskEntity task : taskQueue) {
      canceledTasks.add(task);
    }
    taskQueue.clear();
    return SchedulerResponse.OK(count + " tasks removed.");
  }

  /**
   * Get the list of Tasks, which are grouped by the states.
   */
  public synchronized SchedulerResponse getList() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Running :");
    for (final TaskEntity running : runningTasks) {
      sb.append(" ").append(running.getId());
    }

    sb.append("\nWaiting :");
    for (final TaskEntity waiting : taskQueue) {
      sb.append(" ").append(waiting.getId());
    }

    sb.append("\nFinished :");
    for (final TaskEntity finished : finishedTasks) {
      sb.append(" ").append(finished.getId());
    }

    sb.append("\nCanceled :");
    for (final TaskEntity canceled : canceledTasks) {
      sb.append(" ").append(canceled.getId());
    }
    return SchedulerResponse.OK(sb.toString());
  }

  /**
   * Get the status of a Task.
   */
  public synchronized SchedulerResponse getTaskStatus(final int taskId) {

    for (final TaskEntity running : runningTasks) {
      if (taskId == running.getId()) {
        return SchedulerResponse.OK("Running : " + running.toString());
      }
    }

    for (final TaskEntity waiting : taskQueue) {
      if (taskId == waiting.getId()) {
        return SchedulerResponse.OK("Waiting : " + waiting.toString());
      }
    }

    for (final TaskEntity finished : finishedTasks) {
      if (taskId == finished.getId()) {
        return SchedulerResponse.OK("Finished : " + finished.toString());
      }
    }

    for (final TaskEntity finished : canceledTasks) {
      if (taskId == finished.getId()) {
        return SchedulerResponse.OK("Canceled: " + finished.toString());
      }
    }
    return SchedulerResponse.NOT_FOUND(new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString());
  }

  /**
   * Assigns a TaskId to submit.
   */
  public synchronized int assignTaskId() {
    return taskCount.incrementAndGet();
  }

  /**
   * Add a task to the queue.
   */
  public synchronized void addTask(TaskEntity task) {
    taskQueue.add(task);
  }

  /**
   * Check whether there are tasks waiting to be submitted.
   */
  public synchronized boolean hasPendingTasks() {
    return !taskQueue.isEmpty();
  }

  /**
   * Get the number of pending tasks in the queue.
   */
  public synchronized int getNumPendingTasks() {
    return taskQueue.size();
  }

  /**
   * Update the record of task to mark it as finished.
   */
  public synchronized void setFinished(final int taskId) {
    final TaskEntity task = getTask(taskId, runningTasks);
    runningTasks.remove(task);
    finishedTasks.add(task);
  }

  /**
   * Iterate over the collection to find a TaskEntity with ID.
   */
  private TaskEntity getTask(final int taskId, final Collection<TaskEntity> tasks) {
    TaskEntity result = null;
    for (final TaskEntity task : tasks) {
      if (taskId == task.getId()) {
        result = task;
        break;
      }
    }
    return result;
  }
}

