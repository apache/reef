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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.common.KryoUtils;
import org.apache.reef.vortex.evaluator.VortexWorker;
import org.apache.reef.vortex.protocol.workertomaster.WorkerReport;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());
  private static final int MAX_NUM_OF_FAILURES = 5;
  private static final int SCHEDULER_EVENT = 0; // Dummy number to comply with onNext() interface

  private final AtomicInteger numberOfFailures = new AtomicInteger(0);
  private final EvaluatorRequestor evaluatorRequestor; // for requesting resources
  private final VortexMaster vortexMaster; // Vortex Master
  private final VortexRequestor vortexRequestor; // For sending Commands to remote workers

  // Resource configuration for single thread pool
  private final int evalMem;
  private final int evalNum;
  private final int evalCores;

  private final EStage<VortexStart> vortexStartEStage;
  private final VortexStart vortexStart;
  private final EStage<Integer> pendingTaskletSchedulerEStage;
  private final KryoUtils kryoUtils;

  @Inject
  private VortexDriver(final EvaluatorRequestor evaluatorRequestor,
                       final VortexRequestor vortexRequestor,
                       final VortexMaster vortexMaster,
                       final VortexStart vortexStart,
                       final VortexStartExecutor vortexStartExecutor,
                       final PendingTaskletLauncher pendingTaskletLauncher,
                       final KryoUtils kryoUtils,
                       @Parameter(VortexMasterConf.WorkerMem.class) final int workerMem,
                       @Parameter(VortexMasterConf.WorkerNum.class) final int workerNum,
                       @Parameter(VortexMasterConf.WorkerCores.class) final int workerCores,
                       @Parameter(VortexMasterConf.NumberOfVortexStartThreads.class) final int numOfStartThreads) {
    this.vortexStartEStage = new ThreadPoolStage<>(vortexStartExecutor, numOfStartThreads);
    this.vortexStart = vortexStart;
    this.pendingTaskletSchedulerEStage = new SingleThreadStage<>(pendingTaskletLauncher, 1);
    this.kryoUtils = kryoUtils;
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
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Initial Evaluator Request
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(evalNum)
          .setMemory(evalMem)
          .setNumberOfCores(evalCores)
          .build());

      // Run Vortex Start
      vortexStartEStage.onNext(vortexStart);

      // Run Scheduler
      pendingTaskletSchedulerEStage.onNext(SCHEDULER_EVENT);
    }
  }

  /**
   * Container allocated.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
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
  final class RunningTaskHandler implements EventHandler<RunningTask> {
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
      final WorkerReport workerReport = (WorkerReport)kryoUtils.deserialize(taskMessage.get());
      vortexMaster.workerReported(workerId, workerReport);
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
      if (numberOfFailures.incrementAndGet() >= MAX_NUM_OF_FAILURES) {
        throw new RuntimeException("Exceeded max number of failures");
      } else {
        // We request a new evaluator to take the place of the preempted one
        evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(evalMem)
            .setNumberOfCores(evalCores)
            .build());

        vortexMaster.workerPreempted(failedEvaluator.getFailedTask().get().getId());
      }
    }
  }
}
