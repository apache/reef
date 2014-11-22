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
   * Task queue containing a set of {@link TaskEntity}
   */
  private final Queue<TaskEntity> taskQueue;

  /**
   * Each collection handles the taskIds according to the task status.
   * runningTaskIds is better to be a Set because removal happens
   * when a task finishes. Lists should be enough for the other cases.
   */
  private final Set<Integer> runningTaskIds = new HashSet<>();
  private final List<Integer> finishedTaskIds = new ArrayList<>();
  private final List<Integer> canceledTaskIds = new ArrayList<>();

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
    runningTaskIds.add(taskId);
  }

  /**
   * Update the record of task to mark it as canceled.
   */
  public synchronized SchedulerResponse cancelTask(final int taskId) {
    if (runningTaskIds.contains(taskId)) {
      return SchedulerResponse.FORBIDDEN("The task is running");
    } else if (finishedTaskIds.contains(taskId)) {
      return SchedulerResponse.FORBIDDEN("Already finished");
    }

    for (final TaskEntity task : taskQueue) {
      if (taskId == task.getId()) {
        taskQueue.remove(task);
        canceledTaskIds.add(taskId);
        return SchedulerResponse.OK("Canceled");
      }
    }
    final String message = new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString();
    return SchedulerResponse.NOT_FOUND(message);
  }

  /**
   * Clear the pending list
   */
  public synchronized SchedulerResponse clear() {
    final int count = taskQueue.size();
    for (TaskEntity task : taskQueue) {
      canceledTaskIds.add(task.getId());
    }
    taskQueue.clear();
    return SchedulerResponse.OK(count + " tasks removed.");
  }

  /**
   * Get the list of Tasks. They are classified as their states.
   */
  public synchronized SchedulerResponse getList() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Running :");
    for (final int taskId : runningTaskIds) {
      sb.append(" ").append(taskId);
    }

    sb.append("\nWaiting :");
    for (final TaskEntity task : taskQueue) {
      sb.append(" ").append(task.getId());
    }

    sb.append("\nFinished :");
    for (final int taskId : finishedTaskIds) {
      sb.append(" ").append(taskId);
    }

    sb.append("\nCanceled :");
    for (final int taskId : canceledTaskIds) {
      sb.append(" ").append(taskId);
    }
    return SchedulerResponse.OK(sb.toString());
  }

  /**
   * Get the status of a Task.
   */
  public synchronized SchedulerResponse getTaskStatus(final int taskId) {
    if (runningTaskIds.contains(taskId)) {
      return SchedulerResponse.OK("Running");
    } else if (finishedTaskIds.contains(taskId)) {
      return SchedulerResponse.OK("Finished");
    } else if (canceledTaskIds.contains(taskId)) {
      return SchedulerResponse.OK("Canceled");
    }

    for (final TaskEntity task : taskQueue) {
      if (taskId == task.getId()) {
        return SchedulerResponse.OK("Waiting");
      }
    }
    final String message = new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString();
    return SchedulerResponse.NOT_FOUND(message);
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
    runningTaskIds.remove(taskId);
    finishedTaskIds.add(taskId);
  }
}
