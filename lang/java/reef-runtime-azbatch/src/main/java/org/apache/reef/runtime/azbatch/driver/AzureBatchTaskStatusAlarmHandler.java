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
package org.apache.reef.runtime.azbatch.driver;

import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.TaskState;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchTaskStatusCheckPeriod;
import org.apache.reef.runtime.azbatch.util.batch.AzureBatchHelper;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that gets that status of the tasks from Azure Batch for the job that is currently in progress
 * and notifies REEF of the status.
 * Unlike YARN, Azure Batch does not support Resource Manager callbacks. Therefore, we must periodically call
 * Azure Batch APIs to determine the status of tasks running inside our job.
 */
@Private
@DriverSide
final class AzureBatchTaskStatusAlarmHandler implements EventHandler<Alarm> {

  /**
   * A placeholder remote ID which is used for reporting failed containers when the
   * failure occurs before Wake communication channel can be established and the real
   * remote ID is unknown.
   */
  private static final String PLACEHOLDER_REMOTE_ID = "socket://0.0.0.0:0";

  private final InjectionFuture<AzureBatchEvaluatorShimManager> evaluatorShimManager;
  private final AzureBatchHelper azureBatchHelper;
  private final int taskStatusCheckPeriod;
  private boolean isAlarmEnabled;
  private final Evaluators evaluators;
  private final Clock clock;

  private static final Logger LOG = Logger.getLogger(AzureBatchTaskStatusAlarmHandler.class.getName());

  @Inject
  private AzureBatchTaskStatusAlarmHandler(
      final InjectionFuture<AzureBatchEvaluatorShimManager> evaluatorShimManager,
      final AzureBatchHelper azureBatchHelper,
      final Evaluators evaluators,
      final Clock clock,
      @Parameter(AzureBatchTaskStatusCheckPeriod.class) final int taskStatusCheckPeriod) {
    this.evaluatorShimManager = evaluatorShimManager;
    this.azureBatchHelper = azureBatchHelper;
    this.evaluators = evaluators;
    this.clock = clock;
    this.taskStatusCheckPeriod = taskStatusCheckPeriod;
  }

  /**
   * This method is periodically invoked by the Runtime Clock. It will call Azure Batch APIs to determine
   * the status of tasks running inside the job and notify REEF of tasks statuses that correspond to running
   * evaluators.
   *
   * @param alarm alarm object.
   */
  @Override
  public void onNext(final Alarm alarm) {
    String jobId = this.azureBatchHelper.getAzureBatchJobId();
    List<CloudTask> allTasks = this.azureBatchHelper.getTaskStatusForJob(jobId);

    // Report status if the task has an associated active container.
    LOG.log(Level.FINER, "Found {0} tasks from job id {1}", new Object[]{allTasks.size(), jobId});
    for (CloudTask task : allTasks) {
      Optional<EvaluatorManager> optionalEvaluatorManager = this.evaluators.get(task.id());

      if (!optionalEvaluatorManager.isPresent() && !TaskState.COMPLETED.equals(task.state())) {
        // This usually means that the evaluator shim has started, but hasn't sent the status message
        // back to the driver yet.
        LOG.log(Level.FINE, "No Evaluator found for Azure Batch task id = {0}. Ignoring.", task.id());
      } else if (!optionalEvaluatorManager.isPresent() && TaskState.COMPLETED.equals(task.state())) {
        // This indicates that the evaluator shim exited prematurely. We inform REEF of resource allocation
        // so it's possible to trigger an event signaling resource failure later.
        LOG.log(Level.INFO, "Azure Batch task id = {0} is in 'COMPLETED' state, but it does not have " +
            "an Evaluator associated with it. This indicates that the evaluator shim has failed before " +
            "it could send a callback to the driver.", task.id());
        this.evaluatorShimManager.get().onResourceAllocated(task.id(), PLACEHOLDER_REMOTE_ID, Optional.of(task));
      } else if (optionalEvaluatorManager.get().isClosedOrClosing()) {
        LOG.log(Level.FINE, "Evaluator id = {0} is closed. Ignoring.", task.id());
      } else {
        LOG.log(Level.FINE, "Reporting status for Task Id: {0} is [Azure Batch Status]:{1} ",
            new Object[]{task.id(), task.state().toString()});
        this.evaluatorShimManager.get().onAzureBatchTaskStatus(task);
      }
    }

    if (this.isAlarmEnabled()) {
      this.scheduleAlarm();
    }
  }

  /**
   * Enable the period alarm to send status updates.
   */
  public synchronized void enableAlarm() {
    if (!this.isAlarmEnabled) {
      LOG.log(Level.FINE, "Enabling the alarm and scheduling it to fire in {0} ms.", this.taskStatusCheckPeriod);
      this.isAlarmEnabled = true;
      this.scheduleAlarm();
    } else {
      LOG.log(Level.FINE, "Alarm is already enabled.");
    }
  }

  /**
   * Disable the period alarm to send status updates.
   */
  public synchronized void disableAlarm() {
    this.isAlarmEnabled = false;
  }

  private synchronized boolean isAlarmEnabled() {
    return this.isAlarmEnabled;
  }

  private void scheduleAlarm() {
    this.clock.scheduleAlarm(this.taskStatusCheckPeriod, this);
  }
}
