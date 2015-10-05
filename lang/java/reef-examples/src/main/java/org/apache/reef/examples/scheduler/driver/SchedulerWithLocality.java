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
package org.apache.reef.examples.scheduler.driver;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.examples.scheduler.driver.exceptions.NotFoundException;
import org.apache.reef.examples.scheduler.driver.exceptions.UnsuccessfulException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The body of Task scheduler. It owns a task queue and tracks the record of
 * scheduled tasks.
 */
@ThreadSafe
final class SchedulerWithLocality {

  /**
   * Tasks are waiting to be scheduled in the queue.
   */
  private final Queue<TaskPathEntity> taskQueue;

  /**
   * Lists of {@link TaskPathEntity} for different states - Running / Finished /
   * Canceled.
   */
  private final List<TaskPathEntity> runningTasks = new ArrayList<>();
  private final List<TaskPathEntity> finishedTasks = new ArrayList<>();
  private final List<TaskPathEntity> canceledTasks = new ArrayList<>();

  /**
   * Counts how many tasks have been scheduled.
   */
  private final AtomicInteger taskCount = new AtomicInteger(0);

  @Inject
  private SchedulerWithLocality() {
    taskQueue = new LinkedBlockingQueue<>();
  }

  /**
   * Submit a task to the ActiveContext.
   */
  public synchronized void submitTask(final ActiveContext context) {
    final TaskPathEntity task = taskQueue.poll();
    final Integer taskId = task.getTask().getId();
    final String command = task.getTask().getCommand();

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
  public synchronized int cancelTask(final int taskId) throws UnsuccessfulException, NotFoundException {
    if (getTask(taskId, runningTasks) != null) {
      throw new UnsuccessfulException("The task " + taskId + " is running");
    } else if (getTask(taskId, finishedTasks) != null) {
      throw new UnsuccessfulException("The task " + taskId + " has finished");
    }

    final TaskPathEntity task = getTask(taskId, taskQueue);
    if (task == null) {
      final String message
              = new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString();
      throw new NotFoundException(message);
    } else {
      taskQueue.remove(task);
      canceledTasks.add(task);
      return taskId;
    }
  }

  /**
   * Clear the pending list.
   *
   * @return the number of removed tasks
   */
  public synchronized int clear() {
    final int count = taskQueue.size();
    for (final TaskPathEntity task : taskQueue) {
      canceledTasks.add(task);
    }
    taskQueue.clear();
    return count;
  }

  /**
   * Get the list of Tasks, which are grouped by the states.
   *
   * @return a Map grouped by task state, each with a List of taskIds.
   */
  public synchronized Map<String, List<Integer>> getList() {
    final Map<String, List<Integer>> tasks = new LinkedHashMap<>();

    tasks.put("Running", getTaskIdList(runningTasks));
    tasks.put("Waiting", getTaskIdList(taskQueue));
    tasks.put("Finished", getTaskIdList(finishedTasks));
    tasks.put("Canceled", getTaskIdList(canceledTasks));

    return tasks;
  }

  /**
   * Get the status of a Task.
   *
   * @return the status of the Task.
   */
  public synchronized String getTaskStatus(final int taskId) throws NotFoundException {

    final TaskPathEntity running = getTask(taskId, runningTasks);
    if (running != null) {
      return "Running : " + running.toString();
    }

    final TaskPathEntity waiting = getTask(taskId, taskQueue);
    if (waiting != null) {
      return "Waiting : " + waiting.toString();
    }

    final TaskPathEntity finished = getTask(taskId, finishedTasks);
    if (finished != null) {
      return "Finished : " + finished.toString();
    }

    final TaskPathEntity canceled = getTask(taskId, canceledTasks);
    if (canceled != null) {
      return "Canceled: " + canceled.toString();
    }

    throw new NotFoundException(
            new StringBuilder().append("Task with ID ").append(taskId).append(" is not found").toString());
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
  public synchronized void addTask(final TaskPathEntity task) {
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
    final TaskPathEntity task = getTask(taskId, runningTasks);
    runningTasks.remove(task);
    finishedTasks.add(task);
  }

  /**
   * @return A list of taskIds in the TaskPathEntity collection.
   */
  private static List<Integer> getTaskIdList(final Collection<TaskPathEntity> tasks) {
    final List<Integer> taskIdList = new ArrayList<>(tasks.size());
    for (final TaskPathEntity task : tasks) {
      taskIdList.add(task.getTask().getId());
    }
    return taskIdList;
  }

  /**
   * Iterate over the collection to find a TaskPathEntity with ID.
   */
  private static TaskPathEntity getTask(final int taskId, final Collection<TaskPathEntity> tasks) {
    for (final TaskPathEntity task : tasks) {
      if (taskId == task.getTask().getId()) {
        return task;
      }
    }
    return null;
  }
}
