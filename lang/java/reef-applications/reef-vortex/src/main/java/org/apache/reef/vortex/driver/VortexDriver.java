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
package org.apache.reef.vortex.driver;

import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.common.TaskletException;
import org.apache.reef.vortex.common.TaskletResult;
import org.apache.reef.vortex.common.WorkerReport;
import org.apache.reef.vortex.evaluator.VortexWorker;
import org.apache.reef.vortex.evaluator.VortexWorkerConf;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());
  private final EvaluatorRequestor evaluatorRequestor; // for requesting resources
  private final VortexMaster vortexMaster; // Vortex Master
  private final VortexRequestor vortexRequestor; // For sending Commands to remote workers

  // Resource configuration for single thread pool
  private final int evalMem;
  private final int evalNum;
  private final int evalCores;

  @Inject
  public VortexDriver(final EvaluatorRequestor evaluatorRequestor,
                      final VortexRequestor vortexRequestor,
                      final VortexMaster vortexMaster,
                      final EStage<VortexStart> vortexStartEStage,
                      final VortexStart vortexStart,
                      @Parameter(VortexMasterConf.WorkerMem.class) final int workerMem,
                      @Parameter(VortexMasterConf.WorkerNum.class) final int workerNum,
                      @Parameter(VortexMasterConf.WorkerCores.class) final int workerCores) {
    vortexStartEStage.onNext(vortexStart);
    this.evaluatorRequestor = evaluatorRequestor;
    this.vortexMaster = vortexMaster;
    this.vortexRequestor = vortexRequestor;
    this.evalMem = workerMem;
    this.evalNum = workerNum;
    this.evalCores = workerCores;
  }

  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Initial Evaluator Request
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(evalNum)
          .setMemory(evalMem)
          .setNumberOfCores(evalCores)
          .build());
    }
  }

  /**
   * Container allocated.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Container allocated");
      final String workerId = allocatedEvaluator.getId() + "_vortex_worker";

      final Configuration workerConfiguration = VortexWorkerConf.CONF
          .set(VortexWorkerConf.NUM_OF_THREADS, evalCores) // NUM_OF_THREADS = evalCores
          .build();

      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, workerId)
          .set(TaskConfiguration.TASK, VortexWorker.class)
          .set(TaskConfiguration.ON_SEND_MESSAGE, VortexWorker.class)
          .set(TaskConfiguration.ON_MESSAGE, VortexWorker.DriverMessageHandler.class)
          .set(TaskConfiguration.ON_CLOSE, VortexWorker.TaskCloseHandler.class)
          .build();

      allocatedEvaluator.submitTask(Configurations.merge(workerConfiguration, taskConfiguration));
    }
  }

  /**
   * Evaluator up and running.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask reefTask) {
      LOG.log(Level.INFO, "Worker up and running");
      vortexMaster.workerAllocated(new VortexWorkerManager(vortexRequestor, reefTask));
    }
  }

  /**
   * Message received.
   */
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      final String workerId = taskMessage.getId();
      final WorkerReport workerReport= (WorkerReport)SerializationUtils.deserialize(taskMessage.get());
      switch (workerReport.getType()) {
      case TaskletResult:
        final TaskletResult taskletResult = (TaskletResult)workerReport;
        vortexMaster.taskletCompleted(workerId, taskletResult.getTaskletId(), taskletResult.getResult());
        break;
      case TaskletException:
        final TaskletException taskletException = (TaskletException)workerReport;
        vortexMaster.taskletErrored(workerId, taskletException.getTaskletId(), taskletException.getException());
        break;
      default:
        throw new RuntimeException("Unknown Report");
      }
    }
  }

  /**
   * Evaluator preempted.
   * TODO[REEF-501]: Distinguish different types of FailedEvaluator in Vortex.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "Evaluator preempted");

      final List<FailedContext> contextList = failedEvaluator.getFailedContextList();
      // DANGER: NullPointerException if no context exists(for whatever strange reasons)
      final EvaluatorDescriptor evaluatorDescriptor = contextList.get(0).getEvaluatorDescriptor();

      // We request a new evaluator to take the place of the preempted one
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(evaluatorDescriptor.getMemory())
          .setNumberOfCores(evaluatorDescriptor.getNumberOfCores())
          .build());

      vortexMaster.workerPreempted(failedEvaluator.getId());
    }
  }
}
